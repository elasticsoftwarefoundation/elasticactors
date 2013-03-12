create keyspace ElasticActors;

use ElasticActors;

create column family ActorSystems
  with column_type = 'Standard'
  and key_validation_class = 'UTF8Type'
  and comparator = 'UTF8Type'
  and default_validation_class = 'UTF8Type';

create column family MessageQueues
  with column_type = 'Standard'
  and key_validation_class = 'UTF8Type'
  and comparator = 'TimeUUIDType'
  and default_validation_class = 'BytesType';

create column family PersistentActors
  with column_type = 'Standard'
  and key_validation_class = 'UTF8Type'
  and comparator = 'UTF8Type'
  and default_validation_class = 'BytesType';