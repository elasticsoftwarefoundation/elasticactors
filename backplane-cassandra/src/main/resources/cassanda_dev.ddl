create keyspace ElasticActors with strategy_options = {datacenter1:1};

use ElasticActors;

create column family PersistentActors
  with column_type = 'Standard'
  and comparator = 'UTF8Type'
  and default_validation_class = 'BytesType'
  and key_validation_class = 'CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)'
  and read_repair_chance = 0.1
  and dclocal_read_repair_chance = 0.0
  and populate_io_cache_on_flush = false
  and gc_grace = 864000
  and min_compaction_threshold = 4
  and max_compaction_threshold = 32
  and replicate_on_write = true
  and compaction_strategy = 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'
  and caching = 'KEYS_ONLY'
  and compression_options = {'sstable_compression' : 'org.apache.cassandra.io.compress.SnappyCompressor'};

create column family ActorSystemEventListeners
  with column_type = 'Standard'
  and comparator = 'UTF8Type'
  and default_validation_class = 'BytesType'
  and key_validation_class = 'CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)'
  and read_repair_chance = 0.1
  and dclocal_read_repair_chance = 0.0
  and populate_io_cache_on_flush = false
  and gc_grace = 864000
  and min_compaction_threshold = 4
  and max_compaction_threshold = 32
  and replicate_on_write = true
  and compaction_strategy = 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'
  and caching = 'KEYS_ONLY'
  and compression_options = {'sstable_compression' : 'org.apache.cassandra.io.compress.SnappyCompressor'};

create column family ScheduledMessages
  with column_type = 'Standard'
  and comparator = 'CompositeType(org.apache.cassandra.db.marshal.LongType,org.apache.cassandra.db.marshal.TimeUUIDType)'
  and default_validation_class = 'BytesType'
  and key_validation_class = 'CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)'
  and read_repair_chance = 0.1
  and dclocal_read_repair_chance = 0.0
  and populate_io_cache_on_flush = false
  and gc_grace = 3600
  and min_compaction_threshold = 4
  and max_compaction_threshold = 32
  and replicate_on_write = true
  and compaction_strategy = 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'
  and caching = 'KEYS_ONLY'
  and compression_options = {'sstable_compression' : 'org.apache.cassandra.io.compress.SnappyCompressor'};
