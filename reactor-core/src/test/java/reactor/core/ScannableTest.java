/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import reactor.core.publisher.Flux;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Stephane Maldini
 */
public class ScannableTest {

	static final Scannable.GenericAttr<String> CUSTOM_STRING = new Scannable.GenericAttr<>("global");

	static final Scannable scannable = key -> {
		if (key == Scannable.IntAttr.BUFFERED) return 1;
		if (key == Scannable.BooleanAttr.TERMINATED) return true;
		if (key == Scannable.ScannableAttr.PARENT) return null;
		if (key == Scannable.ScannableAttr.ACTUAL)
			return (Scannable) k -> (Scannable) k2 -> null;

		return null;
	};

	@Test
	public void unavailableScan() {
		assertThat(Scannable.from("nothing")).isEqualTo(Scannable.Attr.UNAVAILABLE_SCAN);
		assertThat(Scannable.from("nothing").isScanAvailable()).isFalse();
		assertThat(Scannable.from("nothing").inners().count()).isEqualTo(0);
		assertThat(Scannable.from("nothing").parents().count()).isEqualTo(0);
		assertThat(Scannable.from("nothing").actuals().count()).isEqualTo(0);
		assertThat(Scannable.from("nothing").scan(Scannable.BooleanAttr.TERMINATED)).isNull();
		assertThat(Scannable.from("nothing").scanOrDefault(Scannable.IntAttr.BUFFERED, 0)).isEqualTo(0);
		assertThat(Scannable.from("nothing").scan(Scannable.ScannableAttr.ACTUAL)).isNull();
	}

	@Test
	public void meaningfulDefaults() {
		Scannable emptyScannable = key -> null;

		assertThat(emptyScannable.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(0);
		assertThat(emptyScannable.scan(Scannable.IntAttr.CAPACITY)).isEqualTo(0);
		assertThat(emptyScannable.scan(Scannable.IntAttr.PREFETCH)).isEqualTo(0);

		assertThat(emptyScannable.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(0L);

		assertThat(emptyScannable.scan(Scannable.BooleanAttr.CANCELLED)).isNull();
		assertThat(emptyScannable.scan(Scannable.BooleanAttr.DELAY_ERROR)).isFalse();
		assertThat(emptyScannable.scan(Scannable.BooleanAttr.TERMINATED)).isNull();

		assertThat(emptyScannable.scan(Scannable.ThrowableAttr.ERROR)).isNull();

		assertThat(emptyScannable.scan(Scannable.ScannableAttr.ACTUAL)).isNull();
		assertThat(emptyScannable.scan(Scannable.ScannableAttr.PARENT)).isNull();

		Scannable.GenericAttr<String> random = new Scannable.GenericAttr<>("foo");
		assertThat(emptyScannable.scan(random)).isEqualToIgnoringCase("foo");
	}

	@Test
	public void availableScan() {
		assertThat(Scannable.from(scannable)).isEqualTo(scannable);
		assertThat(Scannable.from(scannable).isScanAvailable()).isTrue();
		assertThat(Scannable.from(scannable).inners().count()).isEqualTo(0);
		assertThat(Scannable.from(scannable).parents().count()).isEqualTo(0);
		assertThat(Scannable.from(scannable).actuals().count()).isEqualTo(2);
		assertThat(Scannable.from(scannable).scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
		assertThat(Scannable.from(scannable).scanOrDefault(Scannable.IntAttr.BUFFERED, 0)).isEqualTo(1);
		assertThat(Scannable.from(scannable).scan(Scannable.ScannableAttr.ACTUAL)).isEqualTo(scannable.actuals().findFirst().get());
	}

	@Test
	public void nullScan() {
		assertThat(Scannable.from(null))
				.isNotNull()
				.isSameAs(Scannable.Attr.NULL_SCAN);
	}

	@Test
	public void globalDefaultTakesPrecedenceOverCallDefault() {
		assertThat(scannable.scanOrDefault(CUSTOM_STRING, "bar")).isEqualTo("global");
	}

	@Test
	public void namedFluxTest() {
		Flux<Integer> named1 =
				Flux.range(1, 10)
				    .flatMap(i -> Flux.range(100, i))
				    .namedAs("100s")
				    .hide();

		Flux<Integer> named2 = named1.filter(i -> i % 3 == 0)
		                             .namedAs("multiple of 3 100s")
		                             .hide();

		assertThat(Scannable.from(named1).name()).isEqualTo("100s");
		assertThat(Scannable.from(named2).name()).isEqualTo("multiple of 3 100s");
	}

	@Test
	public void taggedFluxTest() {
		Flux<Integer> tagged1 =
				Flux.range(1, 10)
				    .flatMap(i -> Flux.range(100, i))
				    .taggedAs("1", "One", "Common")
				    .hide();


		Flux<Integer> tagged2 = tagged1.filter(i -> i % 3 == 0)
		                               .taggedAs("2", "Two", "Common")
		                               .hide();

		assertThat(Scannable.from(tagged1).tags())
				.containsExactlyInAnyOrder("1", "One", "Common");

		assertThat(Scannable.from(tagged2).tags())
				.containsExactlyInAnyOrder("1", "One", "Common", "2", "Two");
	}
}
