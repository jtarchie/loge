package loge

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/jtarchie/loge/managers"
)

// minTrigramLen is the shortest keyword the per-segment trigram filter can act
// on: a term under 3 bytes has no trigrams, so no segment could be pruned and
// every segment would be LIKE-scanned. The parser rejects shorter terms to keep
// keyword queries prunable.
const minTrigramLen = 3

// ParseSelector parses a LogQL-style selector into stream-label matchers and an
// optional substring line filter. The accepted grammar is:
//
//	{name op "value", ...} |= "keyword"
//
// where op is one of =, !=, =~, !~ (the four stream-label operators), and the
// optional trailing |= "keyword" is the only supported line filter — a substring
// match (a LIKE scan, with a per-segment binary-fuse trigram filter pruning
// segments that cannot contain it). The brace block may be empty ({}) or omitted
// entirely when only a |= filter is given. The keyword must be at least
// minTrigramLen characters so the trigram filter applies.
func ParseSelector(input string) ([]managers.Matcher, string, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return nil, "", fmt.Errorf("empty selector")
	}

	var block, tail string

	switch {
	case strings.HasPrefix(trimmed, "{"):
		end, err := closingBrace(trimmed)
		if err != nil {
			return nil, "", err
		}

		block = trimmed[1:end]
		tail = trimmed[end+1:]
	case strings.HasPrefix(trimmed, "|"):
		// No brace block: the whole input is a line filter.
		tail = trimmed
	default:
		return nil, "", fmt.Errorf("selector must start with '{...}' label matchers or a |= line filter")
	}

	matchers, err := parseMatchers(block)
	if err != nil {
		return nil, "", err
	}

	line, err := parseLineFilter(tail)
	if err != nil {
		return nil, "", err
	}

	return matchers, line, nil
}

// closingBrace returns the index of the '}' that closes the leading '{' in s,
// ignoring braces inside quoted values.
func closingBrace(s string) (int, error) {
	var inQuote, escaped bool

	for i := 1; i < len(s); i++ {
		c := s[i]

		if inQuote {
			switch {
			case escaped:
				escaped = false
			case c == '\\':
				escaped = true
			case c == '"':
				inQuote = false
			}

			continue
		}

		switch c {
		case '"':
			inQuote = true
		case '}':
			return i, nil
		}
	}

	return 0, fmt.Errorf("missing closing '}' in selector")
}

// parseMatchers parses the comma-separated matcher clauses inside the brace
// block. An empty block yields no matchers.
func parseMatchers(block string) ([]managers.Matcher, error) {
	if strings.TrimSpace(block) == "" {
		return nil, nil
	}

	clauses, err := splitClauses(block)
	if err != nil {
		return nil, err
	}

	matchers := make([]managers.Matcher, 0, len(clauses))

	for _, clause := range clauses {
		matcher, err := parseMatcher(clause)
		if err != nil {
			return nil, err
		}

		matchers = append(matchers, matcher)
	}

	return matchers, nil
}

// splitClauses splits the matcher block on top-level commas, leaving commas
// inside quoted values intact.
func splitClauses(block string) ([]string, error) {
	var (
		clauses       []string
		start         int
		inQuote, escd bool
	)

	for i := 0; i < len(block); i++ {
		c := block[i]

		if inQuote {
			switch {
			case escd:
				escd = false
			case c == '\\':
				escd = true
			case c == '"':
				inQuote = false
			}

			continue
		}

		switch c {
		case '"':
			inQuote = true
		case ',':
			clauses = append(clauses, block[start:i])
			start = i + 1
		}
	}

	if inQuote {
		return nil, fmt.Errorf("unterminated string in selector")
	}

	return append(clauses, block[start:]), nil
}

// parseMatcher parses a single `name op "value"` clause into a Matcher.
func parseMatcher(clause string) (managers.Matcher, error) {
	clause = strings.TrimSpace(clause)
	if clause == "" {
		return managers.Matcher{}, fmt.Errorf("empty label matcher")
	}

	name, op, valuePart, err := splitOp(clause)
	if err != nil {
		return managers.Matcher{}, err
	}

	name = strings.TrimSpace(name)
	if name == "" {
		return managers.Matcher{}, fmt.Errorf("label matcher %q: missing label name", clause)
	}

	value, err := unquote(strings.TrimSpace(valuePart))
	if err != nil {
		return managers.Matcher{}, fmt.Errorf("label matcher %q: %w", clause, err)
	}

	return managers.Matcher{Name: name, Value: value, Type: op}, nil
}

// splitOp finds the matcher operator (longest match first: =~, !~, != before =)
// and returns the name before it, the operator, and the remaining value part.
func splitOp(clause string) (name, op, value string, err error) {
	for i := 0; i < len(clause); i++ {
		switch clause[i] {
		case '=':
			if i+1 < len(clause) && clause[i+1] == '~' {
				return clause[:i], "=~", clause[i+2:], nil
			}

			return clause[:i], "=", clause[i+1:], nil
		case '!':
			if i+1 < len(clause) {
				switch clause[i+1] {
				case '=':
					return clause[:i], "!=", clause[i+2:], nil
				case '~':
					return clause[:i], "!~", clause[i+2:], nil
				}
			}

			return "", "", "", fmt.Errorf("label matcher %q: invalid operator", clause)
		}
	}

	return "", "", "", fmt.Errorf("label matcher %q: missing operator (expected =, !=, =~ or !~)", clause)
}

// unquote parses a double-quoted, possibly escaped value.
func unquote(s string) (string, error) {
	if len(s) < 2 || s[0] != '"' {
		return "", fmt.Errorf("value must be a double-quoted string")
	}

	value, err := strconv.Unquote(s)
	if err != nil {
		return "", fmt.Errorf("invalid quoted value %q: %w", s, err)
	}

	return value, nil
}

// parseLineFilter parses the filter tail after the brace block. Only a single
// |= "keyword" substring filter is supported (the engine has one line filter and
// the trigram index serves substrings); |~, !~ and != line filters are rejected.
func parseLineFilter(tail string) (string, error) {
	rest := strings.TrimSpace(tail)

	var (
		line string
		seen bool
	)

	for rest != "" {
		switch {
		case strings.HasPrefix(rest, "|="):
			if seen {
				return "", fmt.Errorf("only a single |= line filter is supported")
			}

			value, remainder, err := takeQuoted(strings.TrimSpace(rest[2:]))
			if err != nil {
				return "", fmt.Errorf("|= line filter: %w", err)
			}

			if utf8.RuneCountInString(value) < minTrigramLen {
				return "", fmt.Errorf("|= term %q must be at least %d characters to use the trigram index", value, minTrigramLen)
			}

			line, seen, rest = value, true, strings.TrimSpace(remainder)
		case strings.HasPrefix(rest, "|~"), strings.HasPrefix(rest, "!~"), strings.HasPrefix(rest, "!="):
			return "", fmt.Errorf("unsupported line filter %q; only |= is supported", rest[:2])
		default:
			return "", fmt.Errorf("unexpected token in selector near %q", rest)
		}
	}

	return line, nil
}

// takeQuoted reads a leading double-quoted value from s and returns its unquoted
// value plus the remaining input after the closing quote.
func takeQuoted(s string) (value, rest string, err error) {
	if s == "" || s[0] != '"' {
		return "", "", fmt.Errorf("expected a double-quoted value")
	}

	var escaped bool

	for i := 1; i < len(s); i++ {
		c := s[i]

		switch {
		case escaped:
			escaped = false
		case c == '\\':
			escaped = true
		case c == '"':
			unquoted, uerr := strconv.Unquote(s[:i+1])
			if uerr != nil {
				return "", "", fmt.Errorf("invalid quoted value: %w", uerr)
			}

			return unquoted, s[i+1:], nil
		}
	}

	return "", "", fmt.Errorf("unterminated quoted value")
}
