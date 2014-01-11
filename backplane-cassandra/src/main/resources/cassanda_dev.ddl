create keyspace ElasticActors with strategy_options = {datacenter1:1};

use ElasticActors;

create column family PersistentActors
  with column_type = 'Standard'
  and key_validation_class = 'CompositeType(UTF8Type,UTF8Type)'
  and comparator = 'UTF8Type'
  and default_validation_class = 'BytesType';

create column family ScheduledMessages
  with column_type = 'Standard'
  and comparator = 'CompositeType(org.apache.cassandra.db.marshal.LongType,org.apache.cassandra.db.marshal.TimeUUIDType)'
  and default_validation_class = 'BytesType'
  and key_validation_class = 'CompositeType(UTF8Type,UTF8Type)';