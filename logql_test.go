package loge_test

import (
	"github.com/jtarchie/loge"
	"github.com/jtarchie/loge/managers"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ParseSelector", func() {
	DescribeTable("valid selectors",
		func(input string, expectedMatchers []managers.Matcher, expectedLine string) {
			matchers, line, err := loge.ParseSelector(input)
			Expect(err).NotTo(HaveOccurred())
			Expect(line).To(Equal(expectedLine))
			Expect(matchers).To(Equal(expectedMatchers))
		},
		Entry("single exact matcher", `{app="web"}`,
			[]managers.Matcher{{Name: "app", Value: "web", Type: "="}}, ""),
		Entry("all four operators", `{a="x", b=~"r", c!="d", e!~"f"}`,
			[]managers.Matcher{
				{Name: "a", Value: "x", Type: "="},
				{Name: "b", Value: "r", Type: "=~"},
				{Name: "c", Value: "d", Type: "!="},
				{Name: "e", Value: "f", Type: "!~"},
			}, ""),
		Entry("empty block", `{}`, []managers.Matcher(nil), ""),
		Entry("empty block with line filter", `{} |= "timeout"`, []managers.Matcher(nil), "timeout"),
		Entry("line filter only", `|= "timeout"`, []managers.Matcher(nil), "timeout"),
		Entry("matcher and line filter", `{app="web"} |= "index"`,
			[]managers.Matcher{{Name: "app", Value: "web", Type: "="}}, "index"),
		Entry("comma inside quoted value", `{app="a,b"}`,
			[]managers.Matcher{{Name: "app", Value: "a,b", Type: "="}}, ""),
		Entry("escaped quotes in value", `{app="say \"hi\""}`,
			[]managers.Matcher{{Name: "app", Value: `say "hi"`, Type: "="}}, ""),
		Entry("brace value inside quotes", `{path="a}b"} |= "needle"`,
			[]managers.Matcher{{Name: "path", Value: "a}b", Type: "="}}, "needle"),
		Entry("whitespace around tokens", `{ app = "web" }`,
			[]managers.Matcher{{Name: "app", Value: "web", Type: "="}}, ""),
	)

	DescribeTable("invalid selectors",
		func(input string) {
			_, _, err := loge.ParseSelector(input)
			Expect(err).To(HaveOccurred())
		},
		Entry("empty input", ``),
		Entry("missing braces", `app="web"`),
		Entry("unquoted value", `{app=web}`),
		Entry("missing closing brace", `{app="web"`),
		Entry("missing operator", `{app}`),
		Entry("line regex is unsupported", `{a="1"} |~ "err"`),
		Entry("line not-equal is unsupported", `{a="1"} != "err"`),
		Entry("multiple line filters", `{a="1"} |= "abc" |= "xyz"`),
		Entry("keyword below the trigram minimum", `{a="1"} |= "ab"`),
	)
})
