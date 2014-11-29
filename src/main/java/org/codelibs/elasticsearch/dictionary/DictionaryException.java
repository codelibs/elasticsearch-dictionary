package org.codelibs.elasticsearch.dictionary;

public class DictionaryException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public DictionaryException(final String message) {
        super(message);
    }

    public DictionaryException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
