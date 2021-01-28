/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Function;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Switches to a new Publisher generated via a function whenever the upstream produces an
 * item.
 *
 * @param <T> the source value type
 * @param <R> the output value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxSwitchMap<T, R> extends InternalFluxOperator<T, R> {

	final Function<? super T, ? extends Publisher<? extends R>> mapper;

	FluxSwitchMap(Flux<? extends T> source,
			Function<? super T, ? extends Publisher<? extends R>> mapper) {
		super(source);
		this.mapper = Objects.requireNonNull(mapper, "mapper");
	}

	@Override
	public int getPrefetch() {
		return Integer.MAX_VALUE;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super R> actual) {
		//for now switchMap doesn't support onErrorContinue, so the scalar version shouldn't either
		if (FluxFlatMap.trySubscribeScalarMap(source, actual, mapper, false, false)) {
			return null;
		}

		return new SwitchMapMain<T, R>(actual, mapper);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class SwitchMapMain<T, R> implements InnerOperator<T, R> {

		static final Logger logger = Loggers.getLogger(SwitchMapMain.class);

		final Function<? super T, ? extends Publisher<? extends R>> mapper;
		final CoreSubscriber<? super R> actual;

		Subscription s;

		boolean done;

		Throwable error;

		SwitchMapInner<T, R> inner;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<SwitchMapMain> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(SwitchMapMain.class, "requested");

		volatile long state;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<SwitchMapMain> STATE =
				AtomicLongFieldUpdater.newUpdater(SwitchMapMain.class, "state");

		SwitchMapMain(CoreSubscriber<? super R> actual,
				Function<? super T, ? extends Publisher<? extends R>> mapper) {
			this.actual = actual;
			this.mapper = mapper;
		}

		@Override
		public final CoreSubscriber<? super R> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			final long state = this.state;
			if (key == Attr.CANCELLED) return !this.done && state == TERMINATED;
			if (key == Attr.PARENT) return this.s;
			if (key == Attr.TERMINATED) return this.done;
			if (key == Attr.ERROR) return this.error;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return this.requested;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(this.inner);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				this.actual.onSubscribe(this);

				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			if (this.done) {
				Operators.onNextDropped(t, this.actual.currentContext());
				return;
			}

			final SwitchMapInner<T, R> si = this.inner;
			final boolean hasInner = si != null;

			if (!hasInner) {
				final SwitchMapInner<T, R> nsi = new SwitchMapInner<>(this, this.actual, 0);
				this.inner = nsi;
				subscribeInner(t, nsi, 0);
				return;
			}

			final int nextIndex = si.index + 1;
			final SwitchMapInner<T, R> nsi = new SwitchMapInner<>(this, this.actual, nextIndex);

			this.inner = nsi;
			si.nextInner = nsi;
			si.nextElement = t;

			long state = incrementIndex(this);
			if (state == TERMINATED) {
				Operators.onDiscard(t, this.actual.currentContext());
				return;
			}

			final boolean isInnerSubscribed = isInnerSubscribed(state);
			if (isInnerSubscribed) {
				si.cancel();

				if (!isWip(state)) {
					// TODO: add check for Long.MAX_VALUE requested
					final long produced = si.produced;
					if (produced > 0) {
						si.requested = 0;
						si.produced = 0;
						REQUESTED.addAndGet(this, -produced);
					}
					subscribeInner(t, nsi, nextIndex);
				}
			}
		}

		void subscribeInner(T nextElement, SwitchMapInner<T, R> nextInner, int nextIndex) {
			final CoreSubscriber<? super R> actual = this.actual;
			final Context context = actual.currentContext();

			while (nextInner.index != nextIndex) {
				Operators.onDiscard(nextElement, context);
				nextElement = nextInner.nextElement;
				nextInner = nextInner.nextInner;
			}

			Publisher<? extends R> p;
			try {
				p = Objects.requireNonNull(this.mapper.apply(nextElement),
						"The mapper returned a null publisher");
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(this.s, e, nextElement, context));
				return;
			}

			p.subscribe(nextInner);
		}

		@Override
		public void onError(Throwable t) {
			if (this.done) {
				Operators.onErrorDropped(t, this.actual.currentContext());
				return;
			}

			this.done = true;

			final long state = setTerminated(this);
			if (state == TERMINATED) {
				Operators.onErrorDropped(t, this.actual.currentContext());
				return;
			}

			final SwitchMapInner<T, R> inner = this.inner;
			if (inner != null && isInnerSubscribed(state)) {
				inner.cancel();
			}

			this.actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (this.done) {
				return;
			}

//			logger.info("onComplete");

			this.done = true;

			final long state = setCompleted(this);

			if (state == TERMINATED) {
				return;
			}

			final SwitchMapInner<T, R> inner = this.inner;
			if (inner == null || hasInnerCompleted(state)) {
				this.actual.onComplete();
			}
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				long previousRequested = Operators.addCap(REQUESTED, this, n);
				long state = addRequest(this, previousRequested, 1);

				if (state == TERMINATED) {
					return;
				}

				if (hasRequest(state) == 1 && isInnerSubscribed(state) && !hasInnerCompleted(state)) {
					final SwitchMapInner<T, R> inner = this.inner;
					if (inner.index == index(state)) {
//						logger.info("state({}); propagateRequest({})", Long.toBinaryString(state), n);
						inner.request(n);
					}
				}
			}
		}

		@Override
		public void cancel() {
			final long state = setTerminated(this);
			if (state == TERMINATED) {
				return;
			}

			if (!hasCompleted(state)) {
				this.s.cancel();
			}

			final SwitchMapInner<T, R> inner = this.inner;
			if (inner != null && isInnerSubscribed(state) && !hasInnerCompleted(state) && inner.index == index(state)) {
				inner.cancel();
			}
		}
	}

	static final class SwitchMapInner<T, R> implements InnerConsumer<R> {

		static final Logger logger = Loggers.getLogger(SwitchMapInner.class);

		final SwitchMapMain<T, R> parent;
		final CoreSubscriber<? super R> actual;

		final int index;

		Subscription s;
		long         produced;
		long         requested;
		boolean      done;

		T nextElement;
		SwitchMapInner<T, R> nextInner;

		SwitchMapInner(SwitchMapMain<T, R> parent, CoreSubscriber<? super R> actual, int index) {
			this.parent = parent;
			this.actual = actual;
			this.index = index;
		}

		@Override
		public Context currentContext() {
			return parent.currentContext();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return this.index ;
			if (key == Attr.PARENT) return this.parent;
			if (key == Attr.ACTUAL) return this.actual;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return null;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				final int expectedIndex = this.index;
				final SwitchMapMain<T, R> parent = this.parent;
				final long state = setInnerSubscribed(parent, expectedIndex);

				if (state == TERMINATED) {
					s.cancel();
					return;
				}

				final int actualIndex = index(state);
				if (actualIndex != expectedIndex) {
//					logger.info("4 state({})", Long.toBinaryString(state));
					s.cancel();
					parent.subscribeInner(this.nextElement, this.nextInner, actualIndex);
					return;
				}

				if (hasRequest(state) > 0) {
					long requested;
					this.requested = requested = parent.requested;
					if (requested > 0) {
//						logger.info("3 state({})", Long.toBinaryString(state));
						s.request(requested);
					} else {
//						logger.info("4 state({})", Long.toBinaryString(state));
					}
				}
			}
		}

		@Override
		public void onNext(R t) {
			if (this.done) {
				Operators.onNextDropped(t, this.actual.currentContext());
				return;
			}

			final SwitchMapMain<T, R> parent = this.parent;
			final Subscription s = this.s;
			final int expectedIndex = this.index;

			long requested = this.requested;
			long state = setWip(parent, expectedIndex);

			if (state == TERMINATED) {
				Operators.onDiscard(t, this.actual.currentContext());
				return;
			}

			int actualIndex = index(state);
			if (actualIndex != expectedIndex) {
				Operators.onDiscard(t, this.actual.currentContext());
				return;
			}

			this.actual.onNext(t);

			long produced = this.produced + 1;
			this.produced = produced;

			int expectedHasRequest = hasRequest(state);
			if (expectedHasRequest > 1) {
				long actualRequested = parent.requested;
				long toRequestInAddition = actualRequested - requested;
				if (toRequestInAddition > 0) {
					this.requested = requested = actualRequested;
					s.request(toRequestInAddition);
				}
			}

			boolean isDemandFulfilled = produced == requested;
			if (isDemandFulfilled) {
				this.produced = 0;
				this.requested = requested = SwitchMapMain.REQUESTED.addAndGet(parent, -produced);

				produced = 0;
				isDemandFulfilled = requested == 0;
				if (!isDemandFulfilled) {
					s.request(requested);
				}
			}

			for (;;) {
				state = unsetWip(parent, expectedIndex, isDemandFulfilled, expectedHasRequest);

				if (state == TERMINATED) {
					return;
				}

				actualIndex = index(state);
				if (expectedIndex != actualIndex) {
					if (produced > 0) {
						this.produced = 0;
						this.requested = 0;

						SwitchMapMain.REQUESTED.addAndGet(parent, -produced);
					}
					parent.subscribeInner(this.nextElement, this.nextInner, actualIndex);
					return;
				}

				int actualHasRequest = hasRequest(state);
				if (expectedHasRequest < actualHasRequest) {
					long currentRequest = parent.requested;
					long toRequestInAddition = currentRequest - requested;

					if (toRequestInAddition > 0) {
						this.requested = requested = currentRequest;
						s.request(toRequestInAddition);

						if (isDemandFulfilled) {
							expectedHasRequest = actualHasRequest;
							isDemandFulfilled = false;
							continue;
						}
					}
				}

				return;
			}
		}

		@Override
		public void onError(Throwable t) {
			// FIXME: include scenario where main and inner calling onError at the time
			if (this.done) {
				Operators.onErrorDropped(t, this.actual.currentContext());
				return;
			}

			this.done = true;

			final SwitchMapMain<T, R> parent = this.parent;

			final long state = setTerminated(parent);
			if (state == TERMINATED) {
				Operators.onErrorDropped(t, this.actual.currentContext());
				return;
			}

			if (!hasCompleted(state)) {
				parent.s.cancel();
			}

			this.actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (this.done) {
				return;
			}

//			logger.info("onComplete");

			this.done = true;

			final SwitchMapMain<T, R> parent = this.parent;
			final int expectedIndex = this.index;

			long state = setWip(parent, expectedIndex);
			if (state == TERMINATED) {
				return;
			}

			int actualIndex = index(state);
			if (actualIndex != expectedIndex) {
				return;
			}

			final long produced = this.produced;
			if (produced > 0) {
				this.produced = 0;
				this.requested = 0;
				SwitchMapMain.REQUESTED.addAndGet(parent, -produced);
			}

			if (hasCompleted(state)) {
				this.actual.onComplete();
				return;
			}

			state = setInnerCompleted(parent);
			if (state == TERMINATED) {
				return;
			}

			actualIndex = index(state);
			if (expectedIndex != actualIndex) {
				parent.subscribeInner(this.nextElement, this.nextInner, actualIndex);
			} else if (hasCompleted(state)) {
				this.actual.onComplete();
			}
		}

		void request(long n) {
			long requested = this.requested;
			this.requested = Operators.addCap(requested, n);

			this.s.request(n);
		}

		void cancel() {
			this.s.cancel();
		}
	}

	static int INDEX_OFFSET          = 32;
	static int HAS_REQUEST_OFFSET    = 4;
	static long TERMINATED            =
			0b11111111111111111111111111111111_1111111111111111111111111111_1_1_1_1L;
	static long INNER_WIP_MASK        =
			0b00000000000000000000000000000000_0000000000000000000000000000_0_0_0_1L;
	static long INNER_SUBSCRIBED_MASK =
			0b00000000000000000000000000000000_0000000000000000000000000000_0_0_1_0L;
	static long INNER_COMPLETED_MASK  =
			0b00000000000000000000000000000000_0000000000000000000000000000_0_1_0_0L;
	static long COMPLETED_MASK        =
			0b00000000000000000000000000000000_0000000000000000000000000000_1_0_0_0L;
	static long HAS_REQUEST_MASK      =
			0b00000000000000000000000000000000_1111111111111111111111111111_0_0_0_0L;
	static int  MAX_HAS_REQUEST       = 0b0000_1111111111111111111111111111;


	static long setTerminated(SwitchMapMain<?, ?> instance) {
		for (;;) {
			final long state = instance.state;

			if (state == TERMINATED) {
				return TERMINATED;
			}

			if (SwitchMapMain.STATE.compareAndSet(instance, state, TERMINATED)) {
				return state;
			}
		}
	}

	static long setCompleted(SwitchMapMain<?, ?> instance) {
		for (; ; ) {
			final long state = instance.state;

			if (state == TERMINATED) {
				return TERMINATED;
			}

			if ((state & COMPLETED_MASK) == COMPLETED_MASK) {
				return state;
			}

			if (SwitchMapMain.STATE.compareAndSet(instance, state, state | COMPLETED_MASK)) {
				return state;
			}
		}
	}

	static long addRequest(SwitchMapMain<?, ?> instance, long previousRequested, int toAdd) {
		for (;;) {
			long state = instance.state;

			if (state == TERMINATED) {
				return TERMINATED;
			}

			final int hasRequest = hasRequest(state);
			if (hasRequest == 0 && previousRequested > 0) {
				return state;
			}

			final long nextState = state(index(state),
					isWip(state),
					hasRequest + toAdd,
					isInnerSubscribed(state),
					hasCompleted(state),
					hasInnerCompleted(state));
			if (SwitchMapMain.STATE.compareAndSet(instance, state, nextState)) {
				return nextState;
			}
		}
	}

	static long incrementIndex(SwitchMapMain<?, ?> instance) {
		long state = instance.state;

		if (state == TERMINATED) {
			return TERMINATED;
		}

		int nextIndex = nextIndex(state);

		for (; ; ) {
			if (SwitchMapMain.STATE.compareAndSet(instance,
					state,
					state(nextIndex,
							isWip(state),
							hasRequest(state),
							false,
							false,
							false))) {
				return state;
			}

			state = instance.state;

			if (state == TERMINATED) {
				return TERMINATED;
			}
		}
	}

	static long setInnerSubscribed(SwitchMapMain<?, ?> instance, int expectedIndex) {
		for (;;) {
			final long state = instance.state;

			if (state == TERMINATED) {
				return TERMINATED;
			}

			int actualIndex = index(state);
			if (expectedIndex != actualIndex) {
				return state;
			}

			if (SwitchMapMain.STATE.compareAndSet(instance,
					state,
					state(expectedIndex,
							false,
							hasRequest(state),
							true,
							hasCompleted(state),
							false))) {
				return state;
			}
		}
	}

	static long setWip(SwitchMapMain<?, ?> instance, int expectedIndex) {
		for (;;) {
			final long state = instance.state;

			if (state == TERMINATED) {
				return TERMINATED;
			}

			int actualIndex = index(state);
			if (expectedIndex != actualIndex) {
				return state;
			}

			if (SwitchMapMain.STATE.compareAndSet(instance,
					state,
					state(expectedIndex,
							true,
							hasRequest(state),
							true,
							hasCompleted(state),
							false))) {
				return state;
			}
		}
	}

	static long unsetWip(SwitchMapMain<?, ?> instance,
			int expectedIndex,
			boolean isDemandFulfilled,
			int expectedRequest) {
		for (; ; ) {
			final long state = instance.state;
			if (state == TERMINATED) {
				return TERMINATED;
			}

			final int actualIndex = index(state);
			final int actualRequest = hasRequest(state);
			final boolean sameIndex = expectedIndex == actualIndex;
			if (isDemandFulfilled && expectedRequest < actualRequest && sameIndex) {
				return state;
			}

			if (SwitchMapMain.STATE.compareAndSet(instance,
					state,
					state(actualIndex,
							false,
							isDemandFulfilled && (sameIndex || expectedRequest == actualRequest) ? 0 : actualRequest,
							isInnerSubscribed(state),
							hasCompleted(state),
							false))) {
				return state;
			}
		}
	}

	static long setInnerCompleted(SwitchMapMain<?, ?> instance) {
		for (; ; ) {
			final long state = instance.state;

			if (state == TERMINATED) {
				return TERMINATED;
			}

			final boolean isInnerSubscribed = isInnerSubscribed(state);
			if (SwitchMapMain.STATE.compareAndSet(instance,
					state,
					state(index(state),
							false,
							hasRequest(state),
							isInnerSubscribed,
							hasCompleted(state),
							isInnerSubscribed))) {
				return state;
			}
		}
	}

	static long state(int index, boolean wip, int hasRequest, boolean innerSubscribed,
			boolean mainCompleted, boolean innerCompleted) {
		return ((long) index << INDEX_OFFSET)
				| (wip ? INNER_WIP_MASK : 0)
				| ((long) Math.max(Math.min(hasRequest, MAX_HAS_REQUEST), 0) << HAS_REQUEST_OFFSET)
				| (innerSubscribed ? INNER_SUBSCRIBED_MASK : 0)
				| (mainCompleted ? COMPLETED_MASK : 0)
				| (innerCompleted ? INNER_COMPLETED_MASK : 0);
	}

	static boolean isInnerSubscribed(long state) {
		return (state & INNER_SUBSCRIBED_MASK) == INNER_SUBSCRIBED_MASK;
	}

	static boolean hasCompleted(long state) {
		return (state & COMPLETED_MASK) == COMPLETED_MASK;
	}

	static boolean hasInnerCompleted(long state) {
		return (state & INNER_COMPLETED_MASK) == INNER_COMPLETED_MASK;
	}

	static int hasRequest(long state) {
		return (int) (state & HAS_REQUEST_MASK) >> HAS_REQUEST_OFFSET;
	}

	static int index(long state) {
		return (int) (state >>> INDEX_OFFSET);
	}

	static int nextIndex(long state) {
		return ((int) (state >>> INDEX_OFFSET)) + 1;
	}

	static boolean isWip(long state) {
		return (state & INNER_WIP_MASK) == INNER_WIP_MASK;
	}
}
