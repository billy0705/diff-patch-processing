// Shared UTF-8 utilities for codepoint-aware operations
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

// These helpers are intentionally declared in the global namespace so they can be
// used from both the DuckDB extension (namespace duckdb) and the diff library
// (namespace diffpatch) without extra qualification.

static inline size_t Utf8CharLen(unsigned char lead) {
	if (lead < 0x80)
		return 1; // 0xxxxxxx
	if ((lead >> 5) == 0x6)
		return 2; // 110xxxxx
	if ((lead >> 4) == 0xE)
		return 3; // 1110xxxx
	if ((lead >> 3) == 0x1E)
		return 4; // 11110xxx
	return 1;     // invalid lead, treat as single byte to avoid looping
}

static inline bool IsUtf8Continuation(unsigned char c) {
	return (c & 0xC0) == 0x80;
}

// Advance by n UTF-8 codepoints from start_byte and return number of bytes consumed
static inline size_t Utf8AdvanceBytes(const std::string &s, size_t start_byte, size_t n_codepoints) {
	size_t i = start_byte;
	size_t end = s.size();
	size_t advanced = 0;
	while (i < end && n_codepoints > 0) {
		unsigned char lead = static_cast<unsigned char>(s[i]);
		size_t clen = Utf8CharLen(lead);
		if (i + clen > end) {
			// truncated sequence; advance 1 to avoid infinite loop
			clen = 1;
		} else if (clen > 1) {
			// validate continuation bytes; if invalid, fall back to 1-byte advance
			for (size_t k = 1; k < clen; ++k) {
				if (!IsUtf8Continuation(static_cast<unsigned char>(s[i + k]))) {
					clen = 1;
					break;
				}
			}
		}
		i += clen;
		advanced += clen;
		--n_codepoints;
	}
	return advanced;
}

// Count codepoints in a UTF-8 buffer
static inline size_t Utf8CountCodepoints(const char *data, size_t len) {
	size_t i = 0;
	size_t count = 0;
	while (i < len) {
		unsigned char lead = static_cast<unsigned char>(data[i]);
		size_t clen = Utf8CharLen(lead);
		if (i + clen > len)
			clen = 1; // truncated tail
		else if (clen > 1) {
			for (size_t k = 1; k < clen; ++k) {
				if (!IsUtf8Continuation(static_cast<unsigned char>(data[i + k]))) {
					clen = 1;
					break;
				}
			}
		}
		i += clen;
		++count;
	}
	return count;
}

// Decode a UTF-8 string into codepoints and output a parallel array of byte offsets
// byte_offsets has size codepoints+1, with a sentinel at the end equal to s.size()
static inline void Utf8ToCodepoints(const std::string &s, std::vector<uint32_t> &out,
                                    std::vector<size_t> &byte_offsets) {
	out.clear();
	byte_offsets.clear();
	size_t i = 0, n = s.size();
	while (i < n) {
		byte_offsets.push_back(i);
		unsigned char c = static_cast<unsigned char>(s[i]);
		uint32_t cp = 0;
		size_t clen = 1;
		if (c < 0x80) {
			cp = c;
			clen = 1;
		} else if ((c >> 5) == 0x6 && i + 1 < n) {
			unsigned char c1 = static_cast<unsigned char>(s[i + 1]);
			if (IsUtf8Continuation(c1)) {
				cp = ((c & 0x1F) << 6) | (c1 & 0x3F);
				clen = 2;
			} else {
				cp = c;
				clen = 1;
			}
		} else if ((c >> 4) == 0xE && i + 2 < n) {
			unsigned char c1 = static_cast<unsigned char>(s[i + 1]);
			unsigned char c2 = static_cast<unsigned char>(s[i + 2]);
			if (IsUtf8Continuation(c1) && IsUtf8Continuation(c2)) {
				cp = ((c & 0x0F) << 12) | ((c1 & 0x3F) << 6) | (c2 & 0x3F);
				clen = 3;
			} else {
				cp = c;
				clen = 1;
			}
		} else if ((c >> 3) == 0x1E && i + 3 < n) {
			unsigned char c1 = static_cast<unsigned char>(s[i + 1]);
			unsigned char c2 = static_cast<unsigned char>(s[i + 2]);
			unsigned char c3 = static_cast<unsigned char>(s[i + 3]);
			if (IsUtf8Continuation(c1) && IsUtf8Continuation(c2) && IsUtf8Continuation(c3)) {
				cp = ((c & 0x07) << 18) | ((c1 & 0x3F) << 12) | ((c2 & 0x3F) << 6) | (c3 & 0x3F);
				clen = 4;
			} else {
				cp = c;
				clen = 1;
			}
		} else {
			cp = c; // invalid lead, treat as 1-byte
			clen = 1;
		}
		out.push_back(cp);
		i += clen;
	}
	byte_offsets.push_back(n);
}

// Convert a sequence of Unicode code points to a UTF-32 string container.
static inline std::u32string CodepointsToU32String(const std::vector<uint32_t> &codepoints) {
	std::u32string result;
	result.reserve(codepoints.size());
	for (auto cp : codepoints) {
		result.push_back(static_cast<char32_t>(cp));
	}
	return result;
}
