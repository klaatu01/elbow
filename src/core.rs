use std::any::Any;

use async_channel::{Receiver, Sender};

pub struct Context {
    pub input_stream: async_channel::Receiver<Box<dyn Any + Send + Sync>>,
    pub output_stream: async_channel::Sender<Box<dyn Any + Send + Sync>>,
}

impl Context {
    pub fn new(
        input: Receiver<Box<dyn Any + Send + Sync>>,
        output: Sender<Box<dyn Any + Send + Sync>>,
    ) -> Self {
        Context {
            input_stream: input,
            output_stream: output,
        }
    }

    pub async fn input<T: Any + Send + Sync + 'static>(&self) -> Option<T> {
        let boxed = self.input_stream.recv().await.ok()?;
        boxed.downcast::<T>().ok().map(|b| *b)
    }

    pub async fn output<T: Any + Send + Sync + 'static>(&self, data: T) {
        let boxed: Box<dyn Any + Send + Sync> = Box::new(data);
        let _ = self.output_stream.send(boxed).await;
    }
}

pub trait Pipe<Input: Send + Sync + 'static, Output: Send + Sync + 'static>:
    Send + Sync + 'static
{
    fn process(&self, input: Input) -> impl Future<Output = Option<Output>> + Send;

    fn run(self: Box<Self>, context: Context) {
        let pipe = self;
        tokio::spawn(async move {
            while let Some(input) = context.input().await {
                if let Some(output) = pipe.process(input).await {
                    context.output(output).await;
                }
            }
        });
    }
}
