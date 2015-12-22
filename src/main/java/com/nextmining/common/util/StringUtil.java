package com.nextmining.common.util;

import java.lang.Character.UnicodeBlock;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class is string utilities.
 * 
 * @author Younggue Bae
 */
public class StringUtil {
	
	public static final String CHAR_CTRL_BRACKET = ""; // "^]"
	public static final String CHAR_CTRL_G = "";	// "^G"
	
	public static final String URL_PATTERN = "\\b(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
	public static final String HTML_TAG_PATTERN = "<(/)?([a-zA-Z]*)(\\s[a-zA-Z]*=[^>]*)?(\\s)*(/)?>";
	
	public static final String replaceStrings(String text, String regex, String newStr) {
		if (text == null) {
			return null;
		}
	
		Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);

		Matcher matcher = pattern.matcher(text);
		return matcher.replaceAll(newStr);
	}

	public static final String removeUnsupportedKoreanCharacters(String str) {
		for (int i = 0; i < str.length(); i++) {
			char ch = str.charAt(i);
			UnicodeBlock unicodeBlock = UnicodeBlock.of(ch);

			if ( !(Character.isDigit(ch)
					|| UnicodeBlock.HANGUL_SYLLABLES.equals(unicodeBlock)
					|| UnicodeBlock.HANGUL_COMPATIBILITY_JAMO.equals(unicodeBlock)
					|| UnicodeBlock.HANGUL_JAMO.equals(unicodeBlock)
					|| UnicodeBlock.BASIC_LATIN.equals(unicodeBlock))
				) {
				str = str.replace(ch, ' ');
			}
		}

		return str;
	}

	public static final boolean isEmpty(String str) {
		if (str == null || str.trim().equals("")) {
			return true;
		}

		return false;
	}

	public static final String escapeDelimiterChar(String str) {
		if (!isEmpty(str))
			return str.replaceAll("\n", "\\\\n").replaceAll("\r", "\\\\r").replaceAll("\t", "\\\\t");

		return str;
	}

	public static final boolean isAlphabet(char c) {
		if ((0x61 <= c && c <= 0x7A) || (0x41 <= c && c <= 0x5A))
			return true;

		return false;
	}

	public static final boolean isJapanese(char c) {
		if ((0x3040 <= c && c <= 0x309f) || (0x30a0 <= c && c <= 0x30ff) || (0x4e00 <= c && c <= 0x9faf))
			return true;

		return false;
	}

	public static final boolean isJapanese(String s) {
		if (s == null || s.trim().length() == 0) {
			return false;
		}

		for (int i = 0; i < s.length(); i++) {
			char ch = s.charAt(i);
			boolean isJapanese = isJapanese(ch);
			if (!isJapanese) {
				return false;
			}
		}

		return true;
	}

	@SuppressWarnings("serial")
	public static final boolean isChinese(char c) {
		Set<UnicodeBlock> chineseUnicodeBlocks = new HashSet<UnicodeBlock>() {{
	    add(UnicodeBlock.CJK_COMPATIBILITY);
	    add(UnicodeBlock.CJK_COMPATIBILITY_FORMS);
	    add(UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS);
	    add(UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS_SUPPLEMENT);
	    add(UnicodeBlock.CJK_RADICALS_SUPPLEMENT);
	    add(UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION);
	    add(UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS);
	    add(UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A);
	    add(UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_B);
	    add(UnicodeBlock.KANGXI_RADICALS);
	    add(UnicodeBlock.IDEOGRAPHIC_DESCRIPTION_CHARACTERS);
		}};

		if (chineseUnicodeBlocks.contains(UnicodeBlock.of(c))) {
			return true;
		}

		return false;
	}

	public static final boolean isChinese(String s) {
		if (s == null || s.trim().length() == 0) {
			return false;
		}

		for (int i = 0; i < s.length(); i++) {
			char ch = s.charAt(i);
			boolean isChinese = isChinese(ch);
			if (!isChinese) {
				return false;
			}
		}

		return true;
	}

	public static final boolean isAlphabet(String s) {
		if (s == null || s.trim().length() == 0) {
			return false;
		}

		for (int i = 0; i < s.length(); i++) {
			char ch = s.charAt(i);
			boolean isAlphabet = isAlphabet(ch);
			if (!isAlphabet) {
				return false;
			}
		}

		return true;
	}

	public static final boolean containsAlphabet(String s) {
		for (int i = 0; i < s.length(); i++) {
			char ch = s.charAt(i);
			boolean isAlphabet = isAlphabet(ch);
			if (isAlphabet) {
				return true;
			}
		}

		return false;
	}

	public static final boolean isHangul(char c) {
		 if ((0xAC00 <= c && c <= 0xD7A3) || (0x3131 <= c && c <= 0x318E))
			return true;

		return false;
	}

	public static final boolean isHangul(String s) {
		if (s == null || s.trim().length() == 0) {
			return false;
		}

		for (int i = 0; i < s.length(); i++) {
			char ch = s.charAt(i);
			boolean isHangul = isHangul(ch);
			if (!isHangul) {
				return false;
			}
		}

		return true;
	}

	public static final boolean containsHangul(String s) {
		for (int i = 0; i < s.length(); i++) {
			char ch = s.charAt(i);
			boolean isHangul = isHangul(ch);
			if (isHangul) {
				return true;
			}
		}
		return false;
	}

	public static final boolean isNumeric(char c) {
		 if (0x30 <= c && c <= 0x39)
			return true;

		return false;
	}

	public static boolean isNumeric(String s) {
    Pattern pattern = Pattern.compile("[+-]?\\d+");
    return pattern.matcher(s).matches(); 
	} 
	
	public static boolean containsNumber(String s) { 
		for (int i = 0; i < s.length(); i++) {
			char ch = s.charAt(i);
			boolean isNumeric = isNumeric(ch);
			if (isNumeric) {
				return true;
			}
		}
		return false;
	} 
	
	public static boolean isAlphaNumeric(String s) { 
		boolean isNumeric = false;
		boolean isAlphabet = false;
		for (int i = 0; i < s.length(); i++) {
			char ch = s.charAt(i);
			if (!isNumeric(ch) && !isAlphabet(ch)) {
				return false;
			}
			else if (isNumeric(ch)) {
				isNumeric = true;
			}
			else if (isAlphabet(ch)) {
				isAlphabet = true;
			}
		}
		
		if (isNumeric && isAlphabet) {
			return true;
		}
		
		return false;
	} 

	public static boolean isHangulNumeric(String s) { 
		boolean isNumeric = false;
		boolean isHangul = false;
		for (int i = 0; i < s.length(); i++) {
			char ch = s.charAt(i);
			if (!isNumeric(ch) && !isHangul(ch)) {
				return false;
			}
			else if (isNumeric(ch)) {
				isNumeric = true;
			}
			else if (isHangul(ch)) {
				isHangul = true;
			}
		}
		
		if (isNumeric && isHangul) {
			return true;
		}
		
		return false;
	}
	
	public static String[] extractURLs(String s) {
		List<String> urls = new ArrayList<String>();
		
		Pattern pattern = Pattern.compile(URL_PATTERN);

		Matcher matcher = pattern.matcher(s);
		while (matcher.find()) {
			urls.add(matcher.group());
		}
		
		return urls.toArray(new String[urls.size()]);
	}

	public static void main(String[] args) {
		String japanese = "誰か確認上記これらのフ";
		boolean isJapanese = isJapanese(japanese);
		System.out.println("isJapanese? " + isJapanese);
		
		String chinese = "查詢促進民間參與公共建設法（210ＢＯＴ法）";
		chinese = "查詢促進民間參與公共建設法";
		boolean isChinese = isChinese(chinese);
		System.out.println("isChinese? " + isChinese);
	}
	
}
