/*
 * Copyright (c) 2020-Present Pivotal Software Inc, All Rights Reserved.
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

import java.util.concurrent.atomic.AtomicLong;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.IIL_Result;
import org.openjdk.jcstress.infra.results.II_Result;
import org.openjdk.jcstress.infra.results.IZL_Result;
import org.openjdk.jcstress.infra.results.I_Result;
import org.openjdk.jcstress.infra.results.JJJJJJJ_Result;
import org.reactivestreams.Publisher;
import org.slf4j.MDC;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.FluxSwitchMap.SwitchMapMain;
import reactor.test.publisher.TestPublisher;
import reactor.util.Logger;
import reactor.util.Loggers;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

public abstract class FluxSwitchMapStressTest {
	static {
		Loggers.useSl4jLoggers();
	}
	static final Logger logger = Loggers.getLogger(FluxSwitchMapStressTest.class);

	final StressSubscriber<Object> stressSubscriber = new StressSubscriber<>(0);

	final StressSubscription stressSubscription = new StressSubscription(null);

	final SwitchMapMain<Object, Object> switchMapMain =
			new SwitchMapMain<>(stressSubscriber, this::handle);


	abstract Publisher<Object> handle(Object value);

	@JCStressTest
	@Outcome(id = {"1"}, expect = ACCEPTABLE, desc = "Exactly one onComplete")
	@State
	public static class OnCompleteStressTest extends FluxSwitchMapStressTest {


		final TestPublisher<Object> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION, TestPublisher.Violation.CLEANUP_ON_TERMINATE);

		{
			switchMapMain.onNext("1");
			switchMapMain.request(1);
		}

		@Override
		Publisher<Object> handle(Object value) {
			return testPublisher;
		}

		@Actor
		public void outerProducer() {
			switchMapMain.onComplete();
		}

		@Actor
		public void innerProducer() {
			testPublisher.next(1);
			testPublisher.complete();
		}

		@Arbiter
		public void arbiter(I_Result r) {
			r.r1 = stressSubscriber.onCompleteCalls.get();
		}
	}

	@JCStressTest
	@Outcome(id = {"0, true, -1"}, expect = ACCEPTABLE, desc = "Cancellation with no downstream onComplete")
	@Outcome(id = {"0, false, -1"}, expect = ACCEPTABLE, desc = "Upstream completion with inner cancellation with no downstream onComplete")
	@Outcome(id = {"1, false, -1"}, expect = ACCEPTABLE, desc = "Upstream completion with inner completion with onComplete")
	@State
	public static class CancelInnerCompleteStressTest extends FluxSwitchMapStressTest {

		final TestPublisher<Object> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION, TestPublisher.Violation.CLEANUP_ON_TERMINATE);

		{
			switchMapMain.onSubscribe(stressSubscription);
			switchMapMain.onNext("1");
			switchMapMain.request(1);
		}

		@Override
		Publisher<Object> handle(Object value) {
			return testPublisher;
		}

		@Actor
		public void outerProducer() {
			switchMapMain.onComplete();
		}

		@Actor
		public void outerRequest() {
			testPublisher.next(1);
			testPublisher.complete();
		}

		@Actor
		public void outerCancel() {
			switchMapMain.cancel();
		}

		@Arbiter
		public void arbiter(IZL_Result r) {
			r.r1 = stressSubscriber.onCompleteCalls.get();
			r.r2 = stressSubscription.cancelled.get();
			r.r3 = switchMapMain.state;
		}
	}

	@JCStressTest
	@Outcome(id = {"200, 1"}, expect = ACCEPTABLE, desc = "Should produced and remaining requested result in total requested number of elements")
	@State
	public static class RequestAndProduceStressTest0 extends FluxSwitchMapStressTest {

		static AtomicLong counter = new AtomicLong();

		final long id = counter.getAndIncrement();
		final long time = System.nanoTime();
		final TestPublisher<Object> testPublisher =
				TestPublisher.createColdNonCompliant(false, TestPublisher.Violation.CLEANUP_ON_TERMINATE, TestPublisher.Violation.DEFER_CANCELLATION);

		{
			MDC.put("id", String.valueOf(id) + time);
			logger.info("----------START----------");
			switchMapMain.onSubscribe(stressSubscription);
		}

		@Override
		Publisher<Object> handle(Object value) {
			return testPublisher;
		}

		@Actor
		public void outerProducer() {
			MDC.put("id", String.valueOf(id) + time);
			for (int i = 0; i < 10; i++) {
				switchMapMain.onNext(i);
			}
			switchMapMain.onComplete();
		}

		@Actor
		public void innerProducer() {
			MDC.put("id", String.valueOf(id) + time);
			for (int i = 0; i < 20; i++) {
//				logger.info("add next({})", i);
				testPublisher.next(i);
			}
			testPublisher.complete();
		}

		@Actor
		public void outerRequest() {
			MDC.put("id", String.valueOf(id) + time);
			for (int i = 0; i < 200; i++) {
				switchMapMain.request(1);
			}
		}

		@Arbiter
		public void arbiter(II_Result r) {
			MDC.put("id", String.valueOf(id) + time);
			r.r1 = (int) (stressSubscriber.onNextCalls.get() + switchMapMain.requested);
			r.r2 = stressSubscriber.onCompleteCalls.get();

			if (stressSubscriber.onCompleteCalls.get() != 1) {
				logger.info("--FAILED: Result({})--", r);
			} else {
				logger.info("--Result({})--", r);
			}
		}
	}

	@JCStressTest
	@Outcome(id = {"200, 0, 0", "200, 1, 0"}, expect = ACCEPTABLE, desc = "Should produced exactly what was requested")
	@State
	public static class RequestAndProduceStressTest2 extends FluxSwitchMapStressTest {

		static AtomicLong counter = new AtomicLong();

		final TestPublisher<Object> testPublisher =
				TestPublisher.createColdNonCompliant(false, TestPublisher.Violation.CLEANUP_ON_TERMINATE, TestPublisher.Violation.DEFER_CANCELLATION);

		final long id = counter.getAndIncrement();
		final long time = System.nanoTime();

		{
			MDC.put("id", String.valueOf(id) + time);
			logger.info("----------START----------");
			switchMapMain.onSubscribe(stressSubscription);
		}

		@Override
		Publisher<Object> handle(Object value) {
			return testPublisher;
		}

		@Actor
		public void outerProducer() {
			MDC.put("id", String.valueOf(id) + time);
			for (int i = 0; i < 10; i++) {
				switchMapMain.onNext(i);
			}
			switchMapMain.onComplete();
		}

		@Actor
		public void innerProducer() {
			MDC.put("id", String.valueOf(id) + time);
			for (int i = 0; i < 200; i++) {
//				logger.info("add next({})", i);
				testPublisher.next(i);
			}
			testPublisher.complete();
		}

		@Actor
		public void outerRequest() {
			MDC.put("id", String.valueOf(id) + time);
			for (int i = 0; i < 200; i++) {
				switchMapMain.request(1);
			}
		}

		@Arbiter
		public void arbiter(IIL_Result r) {
			MDC.put("id", String.valueOf(id) + time);
			r.r1 = stressSubscriber.onNextCalls.get();
			r.r2 = stressSubscriber.onCompleteCalls.get();
			r.r3 = switchMapMain.requested;
			logger.info("Result({})", r);
		}
	}

	@JCStressTest
	//    onNextCalls
	//              |  onNextDiscarded
	//              |  |  onCompleteCalls
	//              |  |  |  requestedInner1
	//              |  |  |  |  requestedInner2
	//              |  |  |  |  |  remainingRequestedMain
	//              |  |  |  |  |  |
	@Outcome(id = {"1, 0, 0, 1, 1, 1, 4294967314"}, expect = ACCEPTABLE, desc = "inner1.onNext(0) -> main.onNext(1) -> main.request(1) -> inner2.onSubscribe" +
																				" || " +
																				"inner1.onNext(0) -> main.onNext(1) -> inner2.onSubscribe -> inner2.request(1)")
	@Outcome(id = {"1, 0, 0, 2, 1, 1, 4294967314"}, expect = ACCEPTABLE, desc = "some " +
			"extra case when onNext observed added value to REQUESTED -> unsetWip -> " +
			"subscribed to next inner and only after that slow addRequest added extra " +
			"value to hasRequest so it ends up with 2 increments")
	@Outcome(id = {"1, 0, 0, 1, 1, 1, 4294967330"}, expect = ACCEPTABLE)
	@Outcome(id = {"1, 0, 0, 2, 1, 1, 4294967330"}, expect = ACCEPTABLE, desc = "inner1.onNext(0) -> inner1.request(1) -> main.onNext(1) -> inner2.onSubscribe(request(1))")
	@Outcome(id = {"0, 1, 0, 1, 2, 2, 4294967314"}, expect = ACCEPTABLE, desc = "main.onNext(1) -> inner1.discarded(0) -> main.request(1) -> inner2.onSubscribe")
	@Outcome(id = {"0, 1, 0, 1, 1, 2, 4294967330"}, expect = ACCEPTABLE, desc = "main.onNext(1) -> inner1.discarded(0) -> inner2.onSubscribe -> main.request(1)")
	@Outcome(id = {"0, 1, 0, 1, 1, 2, 4294967330"}, expect = ACCEPTABLE, desc = "main.onNext(1) -> inner1.discarded(0) -> inner2.onSubscribe -> main.request(1)")
	@Outcome(id = {"0, 1, 0, 1, 2, 2, 4294967330"}, expect = ACCEPTABLE, desc = "main.onNext(1) -> inner1.discarded(0) -> inner2.onSubscribe(request(2)) | main.request(1)")
	@State
	public static class RequestAndProduceStressTest3 extends FluxSwitchMapStressTest {

		static AtomicLong counter = new AtomicLong();

		final long id = counter.getAndIncrement();
		final long time = System.nanoTime();
		   //    0b1_0000000000000000000000000001_0010L
		long a = 0b1_0000000000000000000000000001_0010L;
		long b = 0b1_0000000000000000000000000010_0010L;

		StressSubscription<Integer> subscription1;
		StressSubscription<Integer> subscription2;

		{
			MDC.put("id", String.valueOf(id) + time);
			logger.info("----------START----------");
			switchMapMain.onSubscribe(stressSubscription);
			switchMapMain.request(1);
			switchMapMain.onNext(0);
		}

		@Override
		@SuppressWarnings("rawtypes")
		Publisher<Object> handle(Object value) {
			if ((int) value == 0) {
				return s -> {
					subscription1 = new StressSubscription<>((CoreSubscriber)s);
					s.onSubscribe(subscription1);
				};
			} else {
				return s -> {
					subscription2 = new StressSubscription<>((CoreSubscriber)s);
					s.onSubscribe(subscription2);
				};
			}
		}

		@Actor
		public void outerProducer() {
			MDC.put("id", String.valueOf(id) + time);
			switchMapMain.onNext(1);
		}

		@Actor
		public void innerProducer() {
			MDC.put("id", String.valueOf(id) + time);
			subscription1.actual.onNext(0);
		}

		@Actor
		public void outerRequest() {
			MDC.put("id", String.valueOf(id) + time);
			switchMapMain.request(1);
		}

		@Arbiter
		public void arbiter(JJJJJJJ_Result r) {
			MDC.put("id", String.valueOf(id) + time);
			r.r1 = stressSubscriber.onNextCalls.get();
			r.r2 = stressSubscriber.onNextDiscarded.get();
			r.r3 = stressSubscriber.onCompleteCalls.get();
			r.r4 = subscription1.requested;
			r.r5 = subscription2.requested;
			r.r6 = switchMapMain.requested;
			r.r7 = switchMapMain.state;
			logger.info("Result({})", r);
		}
	}
}