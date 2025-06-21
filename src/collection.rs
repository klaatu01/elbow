use tokio::time::{Instant, timeout};

use crate::pipeable::{Context, Pipeable};
use crate::{DynAdapter, DynPipeline};
use std::any::Any;
use std::marker::PhantomData;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

pub struct ConcurrentPipeline<I, O, P>
where
    I: Any + Send + Sync + 'static,
    O: Any + Send + Sync + 'static,
    P: Pipeable<I, O> + Send + Sync + 'static,
{
    next: AtomicU64,
    txs: Vec<async_channel::Sender<Box<dyn Any + Send + Sync>>>,
    rxs: Vec<async_channel::Receiver<Box<dyn Any + Send + Sync>>>,
    concurrency: usize,
    _phantom: PhantomData<(I, O)>,
    pipe: P,
}

impl<I, O, P> ConcurrentPipeline<I, O, P>
where
    I: Any + Send + Sync + 'static,
    O: Any + Send + Sync + 'static,
    P: Pipeable<I, O> + Clone + Send + Sync + 'static,
{
    pub fn new(concurrency: usize, pipe: P) -> Self {
        let mut txs = Vec::with_capacity(concurrency);
        let mut rxs = Vec::with_capacity(concurrency);

        for _ in 0..concurrency {
            let (tx, rx) = async_channel::unbounded::<Box<dyn Any + Send + Sync>>();
            txs.push(tx);
            rxs.push(rx);
        }

        ConcurrentPipeline {
            next: AtomicU64::new(0),
            txs,
            rxs,
            concurrency,
            _phantom: PhantomData,
            pipe,
        }
    }
}

impl<I, O, P> Clone for ConcurrentPipeline<I, O, P>
where
    I: Any + Send + Sync + 'static,
    O: Any + Send + Sync + 'static,
    P: Pipeable<I, O> + Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        ConcurrentPipeline::new(self.concurrency, self.pipe.clone())
    }
}

pub struct BatchPipeline<I, O, P>
where
    I: Any + Send + Sync + 'static,
    O: Any + Send + Sync + 'static,
    P: Pipeable<I, O> + Send + Sync + 'static,
{
    batch_size: usize,
    window: Duration,
    pipe: P,
    _phantom: PhantomData<(I, O)>,
}

impl<I, O, P> Clone for BatchPipeline<I, O, P>
where
    I: Any + Send + Sync + 'static,
    O: Any + Send + Sync + 'static,
    P: Pipeable<I, O> + Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        BatchPipeline {
            batch_size: self.batch_size,
            window: self.window,
            pipe: self.pipe.clone(),
            _phantom: PhantomData,
        }
    }
}

pub trait PipelineExt<I, O>: Pipeable<I, O> + Clone + Send + Sync + 'static
where
    I: Any + Send + Sync + 'static,
    O: Any + Send + Sync + 'static,
{
    fn concurrent(self, concurrency: usize) -> ConcurrentPipeline<I, O, Self>
    where
        Self: Sized,
    {
        ConcurrentPipeline::new(concurrency, self)
    }

    fn batch(self, batch_size: usize, window: Duration) -> BatchPipeline<I, O, Self>
    where
        Self: Sized,
    {
        BatchPipeline {
            batch_size,
            window,
            pipe: self,
            _phantom: PhantomData,
        }
    }
}

impl<I, O, P> PipelineExt<I, O> for P
where
    I: Any + Send + Sync + 'static,
    O: Any + Send + Sync + 'static,
    P: Pipeable<I, O> + Clone + Send + Sync + 'static,
{
}

impl<I, O, P> Pipeable<I, O> for ConcurrentPipeline<I, O, P>
where
    I: Any + Send + Sync + 'static,
    O: Any + Send + Sync + 'static,
    P: Pipeable<I, O> + Clone + Send + Sync + 'static,
{
    async fn process(&self, input: I) -> Option<O> {
        let next = self.next.load(std::sync::atomic::Ordering::Relaxed);
        let boxed_item: Box<dyn Any + Send + Sync> = Box::new(input);
        let tx = self
            .txs
            .get(next as usize % self.concurrency)
            .expect("Invalid concurrency index");
        let _ = tx.send(boxed_item).await;
        self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        None
    }

    // **override run** to do N-way fan-out/fan-in
    fn run(self: Box<Self>, ctx: Context) {
        for rx in self.rxs.iter() {
            let worker_pipe = self.pipe.clone();
            let worker_ctx = Context::new(rx.clone(), ctx.output_stream.clone());
            let boxed: Box<dyn DynPipeline> = DynAdapter::new(worker_pipe).into_dyn();
            boxed.run_box(worker_ctx);
        }

        tokio::spawn(async move {
            while let Some(item) = ctx.input::<I>().await {
                self.process(item).await;
            }
        });
    }
}

impl<I, O, P> Pipeable<I, O> for BatchPipeline<I, O, P>
where
    I: Any + Send + Sync + 'static,
    O: Any + Send + Sync + 'static,
    P: Pipeable<I, O> + Send + Sync + 'static,
{
    async fn process(&self, input: I) -> Option<O> {
        self.pipe.process(input).await
    }

    fn run(self: Box<Self>, ctx: Context) {
        let batch_size = self.batch_size;
        let window = self.window;

        tokio::spawn(async move {
            loop {
                let first = match ctx.input::<I>().await {
                    Some(item) => item,
                    None => break,
                };

                let mut batch = Vec::with_capacity(batch_size);
                batch.push(first);
                let start = Instant::now();

                while batch.len() < batch_size {
                    let elapsed = start.elapsed();
                    let remaining = if elapsed < window {
                        window - elapsed
                    } else {
                        break;
                    };
                    match timeout(remaining, ctx.input::<I>()).await {
                        Ok(Some(item)) => batch.push(item),
                        _ => break,
                    }
                }

                let outputs =
                    futures::future::join_all(batch.into_iter().map(|i| self.pipe.process(i)))
                        .await;
                for output in outputs.into_iter().flatten() {
                    ctx.output(output).await;
                }
            }
        });
    }
}
