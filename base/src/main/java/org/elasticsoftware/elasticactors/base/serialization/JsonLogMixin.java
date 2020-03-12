package org.elasticsoftware.elasticactors.base.serialization;

import com.fasterxml.jackson.annotation.JsonFilter;

@JsonFilter(JsonLogFilter.FILTER_NAME)
final class JsonLogMixin {

}
