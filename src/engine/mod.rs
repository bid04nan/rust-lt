pub mod core;
pub mod feeders;
pub mod runner;
pub mod protocols;
pub mod stats;
pub mod reporters;

// Re-export commonly used types
pub use core::{Session, Scenario, LoadModel, FlowStep, ActionExecutor, ActionResult, build_schedule, UserInjector, VirtualUser, VuResult};
pub use feeders::{Feeder, FeederConfig, DataRow, FeederStrategy};
pub use runner::{TestExecutor, TestResult, ExecutorConfig};
pub use protocols::{
    HttpClient, HttpRequest, HttpResponse, HttpClientConfig,
    KafkaProducer, KafkaConsumer, KafkaMessage, KafkaProducerConfig, KafkaConsumerConfig,
};
pub use stats::{
    MetricsCollector, Metric, MetricType, MetricStats, ResponseTimeStats, 
    ThroughputStats, DataTransferStats, TimeSeriesMetrics, TimeWindow,
    ConnectionTimingStats, VuLifecycleStats, VuEvent, VuEventType, VuStateSnapshot,
};
pub use reporters::{
    ConsoleReporter, CsvLogger, CsvRequestLogger, CsvVuStateLogger,
    CsvParser, CsvLiveStats as LiveStats, CsvRequestRecord as RequestRecord, CsvVuStateRecord as VuStateRecord, StatsSummary,
    ParquetLogger, get_global_logger, set_global_logger, ParquetLiveStats,
};
