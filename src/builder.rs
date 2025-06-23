use futures::Stream;
use tokio::time::timeout;

use crate::{Context, DynPipe, DynPipeAdapter, Pipe};
use std::{any::Any, time::Instant};

pub struct Pipeline;

pub struct PipelineMap<I, O, F>
where
    I: Any + Send + Sync + 'static,
    O: Any + Send + Sync + 'static,
    F: Fn(I) -> Option<O> + Send + Sync + 'static,
{
    map_fn: F,
    _phantom: std::marker::PhantomData<(I, O)>,
}

impl<I, O, F> Pipe<I, O> for PipelineMap<I, O, F>
where
    I: Any + Send + Sync + 'static,
    O: Any + Send + Sync + 'static,
    F: Fn(I) -> Option<O> + Send + Sync + 'static,
{
    async fn process(&self, input: I) -> Option<O> {
        (self.map_fn)(input)
    }
}

pub struct PipelineFilter<I, F>
where
    I: Any + Send + Sync + 'static,
    F: Fn(&I) -> bool + Send + Sync + 'static,
{
    filter_fn: F,
    _phantom: std::marker::PhantomData<I>,
}

impl<I, F> PipelineFilter<I, F>
where
    I: Any + Send + Sync + 'static,
    F: Fn(&I) -> bool + Send + Sync + 'static,
{
    pub fn new(filter_fn: F) -> Self {
        PipelineFilter {
            filter_fn,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<I, F> Pipe<I, I> for PipelineFilter<I, F>
where
    I: Any + Send + Sync + 'static,
    F: Fn(&I) -> bool + Send + Sync + 'static,
{
    async fn process(&self, input: I) -> Option<I> {
        if (self.filter_fn)(&input) {
            Some(input)
        } else {
            None
        }
    }
}

pub struct PipelineTee<I>
where
    I: Any + Send + Sync + 'static,
{
    tee_fn: Box<dyn Fn(&I) + Send + Sync>,
}

impl<I> PipelineTee<I>
where
    I: Any + Send + Sync + 'static,
{
    pub fn new(tee_fn: impl Fn(&I) + Send + Sync + 'static) -> Self {
        PipelineTee {
            tee_fn: Box::new(tee_fn),
        }
    }
}

impl<I> Pipe<I, I> for PipelineTee<I>
where
    I: Any + Send + Sync + 'static,
{
    async fn process(&self, input: I) -> Option<I> {
        (self.tee_fn)(&input);
        Some(input)
    }
}

pub struct PipelineChunk<I>
where
    I: Any + Send + Sync + 'static,
{
    window_duration: std::time::Duration,
    chunk_size: usize,
    _phantom: std::marker::PhantomData<I>,
}

impl<I> PipelineChunk<I>
where
    I: Any + Send + Sync + 'static,
{
    pub fn new(window_duration: std::time::Duration, chunk_size: usize) -> Self {
        PipelineChunk {
            window_duration,
            chunk_size,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<I> Pipe<I, Vec<I>> for PipelineChunk<I>
where
    I: Any + Send + Sync + 'static,
{
    async fn process(&self, _: I) -> Option<Vec<I>> {
        None
    }

    fn run(self: Box<Self>, ctx: Context) {
        let chunk_size = self.chunk_size;
        let window_duration = self.window_duration;

        tokio::spawn(async move {
            loop {
                let first = match ctx.input::<I>().await {
                    Some(item) => item,
                    None => break,
                };

                let mut batch = Vec::with_capacity(chunk_size);
                batch.push(first);
                let start = Instant::now();

                while batch.len() < chunk_size {
                    let elapsed = start.elapsed();
                    let remaining = if elapsed < window_duration {
                        window_duration - elapsed
                    } else {
                        break;
                    };
                    match timeout(remaining, ctx.input::<I>()).await {
                        Ok(Some(item)) => batch.push(item),
                        _ => break,
                    }
                }
                ctx.output(batch).await;
            }
        });
    }
}

pub struct PipelineJoin<I>
where
    I: Any + Send + Sync + 'static,
{
    _phantom: std::marker::PhantomData<I>,
}

impl<I> PipelineJoin<I>
where
    I: Any + Send + Sync + 'static,
{
    pub fn new() -> Self {
        PipelineJoin {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<I> Pipe<Vec<I>, I> for PipelineJoin<I>
where
    I: Any + Send + Sync + 'static,
{
    async fn process(&self, _: Vec<I>) -> Option<I> {
        None
    }

    fn run(self: Box<Self>, ctx: Context) {
        tokio::spawn(async move {
            while let Some(batch) = ctx.input::<Vec<I>>().await {
                for item in batch {
                    ctx.output(item).await;
                }
            }
        });
    }
}

pub struct PipelineBuilder;

impl PipelineBuilder {
    pub fn first<I, O>(pipe: impl Pipe<I, O> + 'static) -> PipelineBuilderThen<I, I, O>
    where
        I: Any + Send + Sync + 'static,
        O: Any + Send + Sync + 'static,
    {
        let dyn_pipe: Box<dyn DynPipe> = DynPipeAdapter::new(pipe).into_dyn();
        PipelineBuilderThen {
            pipes: vec![dyn_pipe],
            _phantom: std::marker::PhantomData,
        }
    }
}

pub struct PipelineBuilderThen<Input, NextInput, Output>
where
    Input: Any + Send + Sync + 'static,
    NextInput: Any + Send + Sync + 'static,
    Output: Any + Send + Sync + 'static,
{
    pipes: Vec<Box<dyn DynPipe>>,
    _phantom: std::marker::PhantomData<(Input, NextInput, Output)>,
}

impl<Input, NextInput, Output> PipelineBuilderThen<Input, NextInput, Output>
where
    Input: Any + Send + Sync + 'static,
    NextInput: Any + Send + Sync + 'static,
    Output: Any + Send + Sync + 'static,
{
    pub fn new(pipe: impl Pipe<Input, NextInput> + 'static) -> Self {
        let dyn_pipe: Box<dyn DynPipe> = DynPipeAdapter::new(pipe).into_dyn();
        PipelineBuilderThen {
            pipes: vec![dyn_pipe],
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn then<P, O2>(self, pipe: P) -> PipelineBuilderThen<Input, Output, O2>
    where
        P: Pipe<Output, O2> + Send + Sync + 'static,
        O2: Any + Send + Sync + 'static,
    {
        let dyn_pipe: Box<dyn DynPipe> = DynPipeAdapter::new(pipe).into_dyn();
        let mut pipes = self.pipes;
        pipes.push(dyn_pipe);
        PipelineBuilderThen {
            pipes,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn map<F, O2>(self, map_fn: F) -> PipelineBuilderThen<Input, Output, O2>
    where
        F: Fn(Output) -> Option<O2> + Send + Sync + 'static,
        O2: Any + Send + Sync + 'static,
    {
        let map_pipe = PipelineMap {
            map_fn,
            _phantom: std::marker::PhantomData,
        };

        let dyn_pipe: Box<dyn DynPipe> = DynPipeAdapter::new(map_pipe).into_dyn();
        let mut pipes = self.pipes;
        pipes.push(dyn_pipe);
        PipelineBuilderThen {
            pipes,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn filter<F>(self, filter_fn: F) -> PipelineBuilderThen<Input, Output, Output>
    where
        F: Fn(&Output) -> bool + Send + Sync + 'static,
    {
        let filter_pipe = PipelineFilter::new(filter_fn);
        let dyn_pipe: Box<dyn DynPipe> = DynPipeAdapter::new(filter_pipe).into_dyn();
        let mut pipes = self.pipes;
        pipes.push(dyn_pipe);
        PipelineBuilderThen {
            pipes,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn tee<F>(self, tee_fn: F) -> PipelineBuilderThen<Input, Output, Output>
    where
        F: Fn(&Output) + Send + Sync + 'static,
    {
        let tee_pipe = PipelineTee::new(tee_fn);
        let dyn_pipe: Box<dyn DynPipe> = DynPipeAdapter::new(tee_pipe).into_dyn();
        let mut pipes = self.pipes;
        pipes.push(dyn_pipe);
        PipelineBuilderThen {
            pipes,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn chunk(
        self,
        window_duration: std::time::Duration,
        chunk_size: usize,
    ) -> PipelineBuilderThen<Input, Output, Vec<Output>> {
        let chunk_pipe = PipelineChunk::<Output>::new(window_duration, chunk_size);
        let dyn_pipe: Box<dyn DynPipe> = DynPipeAdapter::new(chunk_pipe).into_dyn();
        let mut pipes = self.pipes;
        pipes.push(dyn_pipe);
        PipelineBuilderThen {
            pipes,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn join(self) -> PipelineBuilderThen<Input, Vec<Output>, Output> {
        let join_pipe = PipelineJoin::<Output>::new();
        let dyn_pipe: Box<dyn DynPipe> = DynPipeAdapter::new(join_pipe).into_dyn();
        let mut pipes = self.pipes;
        pipes.push(dyn_pipe);
        PipelineBuilderThen {
            pipes,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn build(self) -> (PipelineInput<Output>, PipelineOutput<Input>) {
        let n = self.pipes.len();
        let mut txs = Vec::with_capacity(n + 1);
        let mut rxs = Vec::with_capacity(n + 1);

        for _ in 0..=n {
            let (tx, rx) = async_channel::unbounded::<Box<dyn Any + Send + Sync>>();
            txs.push(tx);
            rxs.push(rx);
        }

        for (i, pipe) in self.pipes.into_iter().enumerate() {
            let ctx = Context::new(rxs[i].clone(), txs[i + 1].clone());
            pipe.run_box(ctx);
        }

        let final_receiver = PipelineInput::new(rxs[n].clone());
        let first_sender = PipelineOutput::new(txs[0].clone());
        (final_receiver, first_sender)
    }
}

pub struct PipelineInput<I> {
    input: async_channel::Receiver<Box<dyn Any + Send + Sync>>,
    _phantom: std::marker::PhantomData<I>,
}

impl<I> PipelineInput<I>
where
    I: Any + Send + Sync + 'static,
{
    pub fn new(input: async_channel::Receiver<Box<dyn Any + Send + Sync>>) -> Self {
        PipelineInput {
            input,
            _phantom: std::marker::PhantomData,
        }
    }

    pub async fn next(&self) -> Option<I> {
        let boxed = self.input.recv().await.ok()?;
        boxed.downcast::<I>().ok().map(|b| *b)
    }
}

pub struct PipelineOutput<O> {
    output: async_channel::Sender<Box<dyn Any + Send + Sync>>,
    _phantom: std::marker::PhantomData<O>,
}

impl<O> PipelineOutput<O>
where
    O: Any + Send + Sync + 'static,
{
    pub fn new(output: async_channel::Sender<Box<dyn Any + Send + Sync>>) -> Self {
        PipelineOutput {
            output,
            _phantom: std::marker::PhantomData,
        }
    }

    pub async fn send(
        &self,
        data: O,
    ) -> Result<(), async_channel::SendError<Box<dyn Any + Send + Sync>>> {
        let boxed: Box<dyn Any + Send + Sync> = Box::new(data);
        self.output.send(boxed).await
    }

    pub async fn handle<T>(self, data: T)
    where
        T: Into<Vec<O>> + Send + Sync + 'static,
    {
        tokio::spawn(async move {
            let vec_data: Vec<O> = data.into();
            for item in vec_data {
                if let Err(e) = self.send(item).await {
                    eprintln!("Failed to send item: {:?}", e);
                }
            }
            drop(self.output);
        });
    }

    pub fn handle_stream<S>(self, mut stream: S)
    where
        S: Stream<Item = O> + Send + Unpin + 'static,
    {
        tokio::spawn(async move {
            use futures::StreamExt;
            while let Some(item) = stream.next().await {
                if let Err(e) = self.send(item).await {
                    eprintln!("failed to send: {:?}", e);
                }
            }
            drop(self.output);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pipeline_builder() {
        let pipeline = PipelineBuilder::first(PipelineMap {
            map_fn: |x: i32| Some(x * 2),
            _phantom: std::marker::PhantomData,
        })
        .then(PipelineFilter::new(|x: &i32| *x > 5))
        .tee(|x: &i32| println!("Tee: {}", x))
        .map(|x: i32| Some(x + 1))
        .chunk(std::time::Duration::from_secs(1), 3)
        .join()
        .build();

        let (input, output) = pipeline;

        tokio::spawn(async move {
            for i in 0..10 {
                output.send(i).await.unwrap();
            }
            drop(output);
        });

        while let Some(batch) = input.next().await {
            println!("Received batch: {:?}", batch);
        }
    }
}
