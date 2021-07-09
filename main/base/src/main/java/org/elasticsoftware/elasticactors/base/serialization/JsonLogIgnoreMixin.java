package org.elasticsoftware.elasticactors.base.serialization;

import com.fasterxml.jackson.annotation.JsonFilter;

@JsonFilter(JsonLogIgnoreFilter.FILTER_NAME)
final class JsonLogIgnoreMixin {

}
