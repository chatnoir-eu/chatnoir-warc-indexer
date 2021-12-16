MAPPING = {
    "_source": {
        "enabled": True,
        "excludes": [
            "warc_target_query_string",
            "full_body_*",
            "headings_*"
        ]
    },
    "properties": {
        "uuid": {
            "type": "keyword"
        },
        "warc_record_id": {
            "type": "keyword"
        },
        "warc_trec_id": {
            "type": "keyword"
        },
        "date": {
            "type": "date",
            "format": "date_optional_time"
        },
        "lang": {
            "type": "keyword"
        },
        "body_length": {
            "type": "long"
        },
        "warc_target_uri": {
            "type": "keyword"
        },
        "warc_target_hostname": {
            "type": "text",
            "similarity": "BM25",
            "analyzer": "host_analyzer",
            "fields": {
                "raw": {
                    "type": "keyword"
                }
            }
        },
        "warc_target_path": {
            "type": "text",
            "similarity": "BM25",
            "analyzer": "path_analyzer",
            "fields": {
                "raw": {
                    "type": "keyword"
                }
            }
        },
        "warc_target_query_string": {
            "type": "text",
            "similarity": "BM25",
            "analyzer": "query_analyzer"
        },
        "content_type": {
            "type": "keyword"
        },
        "page_rank": {
            "type": "float"
        },
        "spam_rank": {
            "type": "byte"
        }
    },
    "dynamic_templates": [
        {
            "lang_ar": {
                "path_match": "*_lang_ar",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "ar_analyzer",
                    "similarity": "BM25"
                }
            }
        },
        {
            "lang_bg": {
                "path_match": "*_lang_bg",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "bg_analyzer",
                    "similarity": "BM25"
                }
            }
        },
        {
            "clang_ca": {
                "path_match": "*_lang_ca",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "ca_analyzer",
                    "similarity": "BM25"
                }
            }
        },
        {
            "lang_cs": {
                "path_match": "*_lang_cs",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "cs_analyzer",
                    "similarity": "BM25"
                }
            }
        },
        {
            "lang_da": {
                "path_match": "*_lang_da",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "da_analyzer",
                    "similarity": "BM25"
                }
            }
        },
        {
            "lang_de": {
                "path_match": "*_lang_de",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "de_analyzer",
                    "similarity": "BM25"
                }
            }
        },
        {
            "lang_el": {
                "path_match": "*_lang_el",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "el_analyzer",
                    "similarity": "BM25"
                }
            }
        },
        {
            "lang_en": {
                "path_match": "*_lang_en",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "en_analyzer",
                    "similarity": "BM25"
                }
            }
        },
        {
            "lang_es": {
                "path_match": "*_lang_es",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "es_analyzer",
                    "similarity": "BM25"
                }
            }
        },
        {
            "lang_fa": {
                "path_match": "*_lang_fa",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "fa_analyzer",
                    "similarity": "BM25"
                }
            }
        },
        {
            "lang_fi": {
                "path_match": "*_lang_fi",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "fi_analyzer",
                    "similarity": "BM25"
                }
            }
        },
        {
            "lang_fr": {
                "path_match": "*_lang_fr",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "fr_analyzer",
                    "similarity": "BM25"
                }
            }
        },
        {
            "lang_hu": {
                "path_match": "*_lang_hu",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "hu_analyzer",
                    "similarity": "BM25"
                }
            }
        },
        {
            "lang_it": {
                "path_match": "*_lang_it",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "it_analyzer",
                    "similarity": "BM25"
                }
            }
        },
        {
            "lang_ja": {
                "path_match": "*_lang_ja",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "ja_analyzer",
                    "similarity": "BM25"
                }
            }
        },
        {
            "lang_ko": {
                "path_match": "*_lang_ko",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "ko_analyzer",
                    "similarity": "BM25"
                }
            }
        },
        {
            "lang_lt": {
                "path_match": "*_lang_lt",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "lt_analyzer",
                    "similarity": "BM25"
                }
            }
        },
        {
            "lang_nl": {
                "path_match": "*_lang_nl",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "nl_analyzer",
                    "similarity": "BM25"
                }
            }
        },
        {
            "lang_no": {
                "path_match": "*_lang_no",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "no_analyzer",
                    "similarity": "BM25"
                }
            }
        },
        {
            "lang_pl": {
                "path_match": "*_lang_pl",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "pl_analyzer",
                    "similarity": "BM25"
                }
            }
        },
        {
            "lang_pt": {
                "path_match": "*_lang_pt",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "pt_analyzer",
                    "similarity": "BM25"
                }
            }
        },
        {
            "lang_ro": {
                "path_match": "*_lang_ro",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "ro_analyzer",
                    "similarity": "BM25"
                }
            }
        },
        {
            "lang_ru": {
                "path_match": "*_lang_ru",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "ru_analyzer",
                    "similarity": "BM25"
                }
            }
        },
        {
            "lang_sv": {
                "path_match": "*_lang_sv",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "sv_analyzer",
                    "similarity": "BM25"
                }
            }
        },
        {
            "lang_th": {
                "path_match": "*_lang_th",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "th_analyzer",
                    "similarity": "BM25"
                }
            }
        },
        {
            "lang_tr": {
                "path_match": "*_lang_tr",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "tr_analyzer",
                    "similarity": "BM25"
                }
            }
        },
        {
            "lang_zh": {
                "path_match": "*_lang_zh",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "zh_analyzer",
                    "similarity": "BM25"
                }
            }
        },
        {
            "lang_unknown": {
                "path_match": "*_lang_unknown",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "default_analyzer",
                    "similarity": "BM25"
                }
            }
        }
    ]
}

SETTINGS = {
    "refresh_interval": "-1",
    "analysis": {
        "filter": {
            "host_stop_filter": {
                "type": "stop",
                "stopwords": [
                    "com",
                    "net",
                    "org",
                    "ac",
                    "ad",
                    "ae",
                    "af",
                    "ag",
                    "ai",
                    "al",
                    "am",
                    "an",
                    "ao",
                    "aq",
                    "ar",
                    "as",
                    "at",
                    "au",
                    "aw",
                    "ax",
                    "az",
                    "ba",
                    "bb",
                    "bd",
                    "be",
                    "bf",
                    "bg",
                    "bh",
                    "bi",
                    "bj",
                    "bm",
                    "bn",
                    "bo",
                    "br",
                    "bs",
                    "bt",
                    "bv",
                    "bw",
                    "by",
                    "bz",
                    "ca",
                    "cc",
                    "cd",
                    "cf",
                    "cg",
                    "ch",
                    "ci",
                    "ck",
                    "cl",
                    "cm",
                    "cn",
                    "co",
                    "cr",
                    "cs",
                    "cu",
                    "cv",
                    "cx",
                    "cy",
                    "cz",
                    "dd",
                    "de",
                    "dj",
                    "dk",
                    "dm",
                    "do",
                    "dz",
                    "ec",
                    "ee",
                    "eg",
                    "eh",
                    "er",
                    "es",
                    "et",
                    "eu",
                    "fi",
                    "fj",
                    "fk",
                    "fm",
                    "fo",
                    "fr",
                    "ga",
                    "gb",
                    "gd",
                    "ge",
                    "gf",
                    "gg",
                    "gh",
                    "gi",
                    "gl",
                    "gm",
                    "gn",
                    "gp",
                    "gq",
                    "gr",
                    "gs",
                    "gt",
                    "gu",
                    "gw",
                    "gy",
                    "hk",
                    "hm",
                    "hn",
                    "hr",
                    "ht",
                    "hu",
                    "id",
                    "ie",
                    "il",
                    "im",
                    "in",
                    "io",
                    "iq",
                    "ir",
                    "is",
                    "it",
                    "je",
                    "jm",
                    "jo",
                    "jp",
                    "ke",
                    "kg",
                    "kh",
                    "ki",
                    "km",
                    "kn",
                    "kp",
                    "kr",
                    "kw",
                    "ky",
                    "kz",
                    "la",
                    "lb",
                    "lc",
                    "li",
                    "lk",
                    "lr",
                    "ls",
                    "lt",
                    "lu",
                    "lv",
                    "ly",
                    "ma",
                    "mc",
                    "md",
                    "me",
                    "mg",
                    "mh",
                    "mk",
                    "ml",
                    "mm",
                    "mn",
                    "mo",
                    "mp",
                    "mq",
                    "mr",
                    "ms",
                    "mt",
                    "mu",
                    "mv",
                    "mw",
                    "mx",
                    "my",
                    "mz",
                    "na",
                    "nc",
                    "ne",
                    "nf",
                    "ng",
                    "ni",
                    "nl",
                    "no",
                    "np",
                    "nr",
                    "nu",
                    "nz",
                    "om",
                    "pa",
                    "pe",
                    "pf",
                    "pg",
                    "ph",
                    "pk",
                    "pl",
                    "pm",
                    "pn",
                    "pr",
                    "ps",
                    "pt",
                    "pw",
                    "py",
                    "qa",
                    "re",
                    "ro",
                    "rs",
                    "ru",
                    "rw",
                    "sa",
                    "sb",
                    "sc",
                    "sd",
                    "se",
                    "sg",
                    "sh",
                    "si",
                    "sj",
                    "sk",
                    "sl",
                    "sm",
                    "sn",
                    "so",
                    "sr",
                    "st",
                    "su",
                    "sv",
                    "sy",
                    "sz",
                    "tc",
                    "td",
                    "tf",
                    "tg",
                    "th",
                    "tj",
                    "tk",
                    "tl",
                    "tm",
                    "tn",
                    "to",
                    "tp",
                    "tr",
                    "tt",
                    "tv",
                    "tw",
                    "tz",
                    "ua",
                    "ug",
                    "uk",
                    "um",
                    "us",
                    "uy",
                    "uz",
                    "va",
                    "vc",
                    "ve",
                    "vg",
                    "vi",
                    "vn",
                    "vu",
                    "wf",
                    "ws",
                    "ye",
                    "yt",
                    "yu",
                    "za",
                    "zm",
                    "zr",
                    "zw"
                ]
            },
            "host_pre_filter": {
                "type": "pattern_capture",
                "preserve_original": "false",
                "patterns": [
                    "^(?:www\\d?\\.)(.+\\..+)$"
                ]
            },
            "host_delimiter_filter": {
                "split_on_numerics": "false",
                "generate_word_parts": "true",
                "preserve_original": "true",
                "catenate_words": "true",
                "type": "word_delimiter",
                "catenate_numbers": "true",
                "stem_english_possessive": "true"
            },
            "fr_stem_filter": {
                "name": "minimal_french",
                "type": "stemmer"
            },
            "it_stem_filter": {
                "name": "light_italian",
                "type": "stemmer"
            },
            "de_stem_filter": {
                "name": "minimal_german",
                "type": "stemmer"
            },
            "en_stem_filter": {
                "name": "minimal_english",
                "type": "stemmer"
            },
            "fi_stem_filter": {
                "name": "light_finish",
                "type": "stemmer"
            },
            "ja_pos_filter": {
                "type": "kuromoji_part_of_speech",
                "stoptags": [
                    "\u52a9\u8a5e-\u683c\u52a9\u8a5e-\u4e00\u822c",
                    "\u52a9\u8a5e-\u7d42\u52a9\u8a5e"
                ]
            },
            "es_stem_filter": {
                "name": "light_spanish",
                "type": "stemmer"
            },
            "hu_stem_filter": {
                "name": "light_hungarian",
                "type": "stemmer"
            },
            "pl_stem_filter": {
                "name": "polish_stop",
                "type": "stop"
            },
            "pt_stem_filter": {
                "name": "minimal_portuguese",
                "type": "stemmer"
            },
            "ru_stem_filter": {
                "name": "light_russian",
                "type": "stemmer"
            },
            "sv_stem_filter": {
                "name": "light_swedish",
                "type": "stemmer"
            }
        },
        "analyzer": {
            "host_analyzer": {
                "filter": [
                    "host_pre_filter",
                    "host_delimiter_filter",
                    "host_stop_filter",
                    "en_stem_filter",
                    "unique"
                ],
                "type": "custom",
                "tokenizer": "whitespace"
            },
            "de_analyzer": {
                "filter": [
                    "icu_normalizer",
                    "de_stem_filter",
                    "icu_folding"
                ],
                "type": "custom",
                "tokenizer": "icu_tokenizer"
            },
            "ar_analyzer": {
                "filter": [
                    "icu_normalizer",
                    "icu_folding"
                ],
                "type": "custom",
                "tokenizer": "icu_tokenizer"
            },
            "bg_analyzer": {
                "filter": [
                    "icu_normalizer",
                    "icu_folding"
                ],
                "type": "custom",
                "tokenizer": "icu_tokenizer"
            },
            "fr_analyzer": {
                "filter": [
                    "icu_normalizer",
                    "elision",
                    "fr_stem_filter",
                    "icu_folding"
                ],
                "type": "custom",
                "tokenizer": "icu_tokenizer"
            },
            "it_analyzer": {
                "filter": [
                    "icu_normalizer",
                    "it_stem_filter",
                    "icu_folding"
                ],
                "type": "custom",
                "tokenizer": "icu_tokenizer"
            },
            "cs_analyzer": {
                "filter": [
                    "icu_normalizer",
                    "icu_folding"
                ],
                "type": "custom",
                "tokenizer": "icu_tokenizer"
            },
            "nl_analyzer": {
                "filter": [
                    "icu_normalizer",
                    "icu_folding"
                ],
                "type": "custom",
                "tokenizer": "icu_tokenizer"
            },
            "hu_analyzer": {
                "filter": [
                    "icu_normalizer",
                    "hu_stem_filter",
                    "icu_folding"
                ],
                "type": "custom",
                "tokenizer": "icu_tokenizer"
            },
            "no_analyzer": {
                "filter": [
                    "icu_normalizer",
                    "icu_folding"
                ],
                "type": "custom",
                "tokenizer": "icu_tokenizer"
            },
            "pl_analyzer": {
                "filter": [
                    "icu_normalizer",
                    "icu_folding"
                ],
                "type": "custom",
                "tokenizer": "icu_tokenizer"
            },
            "da_analyzer": {
                "filter": [
                    "icu_normalizer",
                    "icu_folding"
                ],
                "type": "custom",
                "tokenizer": "icu_tokenizer"
            },
            "es_analyzer": {
                "filter": [
                    "icu_normalizer",
                    "es_stem_filter",
                    "icu_folding"
                ],
                "type": "custom",
                "tokenizer": "icu_tokenizer"
            },
            "th_analyzer": {
                "filter": [
                    "icu_normalizer",
                    "icu_folding"
                ],
                "type": "custom",
                "tokenizer": "icu_tokenizer"
            },
            "ru_analyzer": {
                "filter": [
                    "icu_normalizer",
                    "ru_stem_filter",
                    "icu_folding"
                ],
                "type": "custom",
                "tokenizer": "icu_tokenizer"
            },
            "en_analyzer": {
                "filter": [
                    "icu_normalizer",
                    "en_stem_filter",
                    "icu_folding"
                ],
                "type": "custom",
                "tokenizer": "icu_tokenizer"
            },
            "fa_analyzer": {
                "filter": [
                    "icu_normalizer",
                    "icu_folding"
                ],
                "type": "custom",
                "tokenizer": "icu_tokenizer"
            },
            "ko_analyzer": {
                "type": "cjk"
            },
            "lt_analyzer": {
                "filter": [
                    "icu_normalizer",
                    "icu_folding"
                ],
                "type": "custom",
                "tokenizer": "icu_tokenizer"
            },
            "lowercase_analyzer": {
                "filter": [
                    "icu_normalizer",
                    "icu_folding"
                ],
                "type": "custom",
                "tokenizer": "keyword"
            },
            "sv_analyzer": {
                "filter": [
                    "icu_normalizer",
                    "sv_stem_filter",
                    "icu_folding"
                ],
                "type": "custom",
                "tokenizer": "icu_tokenizer"
            },
            "pt_analyzer": {
                "filter": [
                    "icu_normalizer",
                    "pt_stem_filter",
                    "icu_folding"
                ],
                "type": "custom",
                "tokenizer": "icu_tokenizer"
            },
            "ro_analyzer": {
                "filter": [
                    "icu_normalizer",
                    "icu_folding"
                ],
                "type": "custom",
                "tokenizer": "icu_tokenizer"
            },
            "tr_analyzer": {
                "filter": [
                    "icu_normalizer",
                    "icu_folding"
                ],
                "type": "custom",
                "tokenizer": "icu_tokenizer"
            },
            "path_analyzer": {
                "type": "custom",
                "tokenizer": "path_tokenizer"
            },
            "el_analyzer": {
                "filter": [
                    "icu_normalizer",
                    "icu_folding"
                ],
                "type": "custom",
                "tokenizer": "icu_tokenizer"
            },
            "ja_analyzer": {
                "filter": [
                    "kuromoji_baseform",
                    "ja_pos_filter",
                    "icu_normalizer",
                    "icu_folding",
                    "cjk_width"
                ],
                "type": "custom",
                "tokenizer": "kuromoji_tokenizer"
            },
            "fi_analyzer": {
                "filter": [
                    "icu_normalizer",
                    "fi_stem_filter",
                    "icu_folding"
                ],
                "type": "custom",
                "tokenizer": "icu_tokenizer"
            },
            "default_analyzer": {
                "filter": [
                    "icu_normalizer",
                    "icu_folding"
                ],
                "type": "custom",
                "tokenizer": "icu_tokenizer"
            },
            "query_analyzer": {
                "type": "custom",
                "tokenizer": "query_tokenizer"
            },
            "ca_analyzer": {
                "filter": [
                    "icu_normalizer",
                    "icu_folding"
                ],
                "type": "custom",
                "tokenizer": "icu_tokenizer"
            },
            "zh_analyzer": {
                "filter": [
                    "smartcn_word",
                    "icu_normalizer",
                    "icu_folding"
                ],
                "type": "custom",
                "tokenizer": "smartcn_sentence"
            }
        },
        "tokenizer": {
            "query_tokenizer": {
                "pattern": "[&=]",
                "type": "pattern"
            },
            "path_tokenizer": {
                "type": "path_hierarchy",
                "delimiter": "/"
            }
        }
    }
}
