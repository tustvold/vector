use super::{
    fanout::{self, Fanout},
    task::Task,
    ConfigDiff,
};
use crate::{
    buffers,
    config::{DataType, SinkContext, TransformContext},
    dns::Resolver,
    event::Event,
    shutdown::SourceShutdownCoordinator,
    Pipeline,
};
use futures::{
    compat::{Future01CompatExt, Stream01CompatExt},
    future, FutureExt, StreamExt,
};
use futures01::{sync::mpsc, Future, Stream};
use std::collections::HashMap;
use tokio::time::{timeout, Duration};

pub struct Pieces {
    pub inputs: HashMap<String, (buffers::BufferInputCloner, Vec<String>)>,
    pub outputs: HashMap<String, fanout::ControlChannel>,
    pub tasks: HashMap<String, Task>,
    pub source_tasks: HashMap<String, Task>,
    pub healthchecks: HashMap<String, Task>,
    pub shutdown_coordinator: SourceShutdownCoordinator,
}

/// Builds only the new pieces, and doesn't check their topology.
pub async fn build_pieces(
    config: &super::Config,
    diff: &ConfigDiff,
) -> Result<Pieces, Vec<String>> {
    let mut inputs = HashMap::new();
    let mut outputs = HashMap::new();
    let mut tasks = HashMap::new();
    let mut source_tasks = HashMap::new();
    let mut healthchecks = HashMap::new();
    let mut shutdown_coordinator = SourceShutdownCoordinator::default();

    let mut errors = vec![];

    // TODO: remove the unimplemented
    let resolver = Resolver;

    // Build sources
    for (name, source) in config
        .sources
        .iter()
        .filter(|(name, _)| diff.sources.contains_new(&name))
    {
        let (tx, rx) = mpsc::channel(1000);
        let pipeline = Pipeline::from_sender(tx);

        let typetag = source.source_type();

        let (shutdown_signal, force_shutdown_tripwire) = shutdown_coordinator.register_source(name);

        let server = match source
            .build_async(&name, &config.global, shutdown_signal, pipeline)
            .await
        {
            Err(error) => {
                errors.push(format!("Source \"{}\": {}", name, error));
                continue;
            }
            Ok(server) => server,
        };

        let (output, control) = Fanout::new();
        let pump = rx.forward(output).map(|_| ()).compat();
        let pump = Task::new(name, typetag, pump);

        // The force_shutdown_tripwire is a Future that when it resolves means that this source
        // has failed to shut down gracefully within its allotted time window and instead should be
        // forcibly shut down.  We accomplish this by select()-ing on the server Task with the
        // force_shutdown_tripwire.  That means that if the force_shutdown_tripwire resolves while
        // the server Task is still running the Task will simply be dropped on the floor.
        let server = server
            .select(force_shutdown_tripwire)
            .map(|_| debug!("Finished"))
            .map_err(|_| ())
            .compat();
        let server = Task::new(name, typetag, server);

        outputs.insert(name.clone(), control);
        tasks.insert(name.clone(), pump);
        source_tasks.insert(name.clone(), server);
    }

    // Build transforms
    for (name, transform) in config
        .transforms
        .iter()
        .filter(|(name, _)| diff.transforms.contains_new(&name))
    {
        let trans_inputs = &transform.inputs;

        let typetag = transform.inner.transform_type();

        let cx = TransformContext { resolver };

        let input_type = transform.inner.input_type();
        let transform = match transform.inner.build_async(cx).await {
            Err(error) => {
                errors.push(format!("Transform \"{}\": {}", name, error));
                continue;
            }
            Ok(transform) => transform,
        };

        let (input_tx, input_rx) = futures01::sync::mpsc::channel(100);
        let input_tx = buffers::BufferInputCloner::Memory(input_tx, buffers::WhenFull::Block, name.clone());

        let (output, control) = Fanout::new();

        let transform = transform
            .transform_stream(filter_event_type(input_rx, input_type))
            .forward(output)
            .map(|_| debug!("Finished"))
            .compat();
        let task = Task::new(name, typetag, transform);

        inputs.insert(name.clone(), (input_tx, trans_inputs.clone()));
        outputs.insert(name.clone(), control);
        tasks.insert(name.clone(), task);
    }

    // Build sinks
    for (name, sink) in config
        .sinks
        .iter()
        .filter(|(name, _)| diff.sinks.contains_new(&name))
    {
        let sink_inputs = &sink.inputs;
        let enable_healthcheck = sink.healthcheck;

        let typetag = sink.inner.sink_type();
        let input_type = sink.inner.input_type();

        let buffer = sink.buffer.build(&config.global.data_dir, &name);
        let (tx, rx, acker) = match buffer {
            Err(error) => {
                errors.push(format!("Sink \"{}\": {}", name, error));
                continue;
            }
            Ok(buffer) => buffer,
        };

        let cx = SinkContext { resolver, acker };

        let (sink, healthcheck) = match sink.inner.build_async(cx).await {
            Err(error) => {
                errors.push(format!("Sink \"{}\": {}", name, error));
                continue;
            }
            Ok((sink, healthcheck)) => (sink, healthcheck),
        };

        let sink = sink
            .run(
                filter_event_type(rx, input_type)
                    .compat()
                    .take_while(|e| future::ready(e.is_ok()))
                    .map(|x| x.unwrap()),
            )
            .inspect(|_| debug!("Finished"));
        let task = Task::new(name, typetag, sink);

        let healthcheck_task = async move {
            if enable_healthcheck {
                let duration = Duration::from_secs(10);
                timeout(duration, healthcheck)
                    .map(|result| match result {
                        Ok(Ok(_)) => {
                            info!("Healthcheck: Passed.");
                            Ok(())
                        }
                        Ok(Err(error)) => {
                            error!("Healthcheck: Failed Reason: {}", error);
                            Err(())
                        }
                        Err(_) => {
                            error!("Healthcheck: timeout");
                            Err(())
                        }
                    })
                    .await
            } else {
                info!("Healthcheck: Disabled.");
                Ok(())
            }
        };
        let healthcheck_task = Task::new(name, typetag, healthcheck_task);

        inputs.insert(name.clone(), (tx, sink_inputs.clone()));
        healthchecks.insert(name.clone(), healthcheck_task);
        tasks.insert(name.clone(), task);
    }

    if errors.is_empty() {
        let pieces = Pieces {
            inputs,
            outputs,
            tasks,
            source_tasks,
            healthchecks,
            shutdown_coordinator,
        };

        Ok(pieces)
    } else {
        Err(errors)
    }
}

fn filter_event_type<S>(
    stream: S,
    data_type: DataType,
) -> Box<dyn Stream<Item = Event, Error = ()> + Send>
where
    S: Stream<Item = Event, Error = ()> + Send + 'static,
{
    match data_type {
        DataType::Any => Box::new(stream), // it's possible to not call any comparing function if any type is supported
        DataType::Log => Box::new(stream.filter(|event| match event {
            Event::Log(_) => true,
            _ => false,
        })),
        DataType::Metric => Box::new(stream.filter(|event| match event {
            Event::Metric(_) => true,
            _ => false,
        })),
    }
}
