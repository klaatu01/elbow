use std::any::Any;

use async_channel::{Receiver, Sender};
use futures::StreamExt;

pub struct PipeInput<Input: Send + Sync + 'static> {
    input: async_channel::Receiver<Input>,
}

impl<Input: Send + Sync + 'static> PipeInput<Input> {
    pub fn new(input: async_channel::Receiver<Input>) -> Self {
        PipeInput { input }
    }

    pub async fn next(&self) -> Option<Input> {
        self.input.recv().await.ok()
    }
}

pub fn pair<T: Send + Sync + 'static>() -> (PipeInput<T>, PipeOutput<T>) {
    let (sender, receiver) = async_channel::unbounded();
    (PipeInput::new(receiver), PipeOutput::new(sender))
}

pub struct PipeOutput<Output: Send + Sync + 'static> {
    output: async_channel::Sender<Output>,
}

impl<Output: Send + Sync + 'static> PipeOutput<Output> {
    pub fn new(output: async_channel::Sender<Output>) -> Self {
        PipeOutput { output }
    }

    pub async fn send(&self, data: Output) -> Result<(), async_channel::SendError<Output>> {
        self.output.send(data).await
    }
}

pub struct Context {
    input: async_channel::Receiver<Box<dyn Any + Send + Sync>>,
    output: async_channel::Sender<Box<dyn Any + Send + Sync>>,
}

impl Context {
    pub fn new(
        input: Receiver<Box<dyn Any + Send + Sync>>,
        output: Sender<Box<dyn Any + Send + Sync>>,
    ) -> Self {
        Context { input, output }
    }

    pub async fn input<T: Any + Send + Sync + 'static>(&self) -> Option<T> {
        let boxed = self.input.recv().await.ok()?;
        boxed.downcast::<T>().ok().map(|b| *b)
    }

    pub async fn output<T: Any + Send + Sync + 'static>(&self, data: T) {
        let boxed: Box<dyn Any + Send + Sync> = Box::new(data);
        let _ = self.output.send(boxed).await;
    }
}

pub trait Pipeable<Input: Send + Sync + 'static, Output: Send + Sync + 'static>:
    Send + Sync + 'static
{
    fn process(&self, input: Input) -> impl Future<Output = Output> + Send;

    fn run(self: Box<Self>, context: Context) {
        let pipe = self;
        tokio::spawn(async move {
            while let Some(input) = context.input().await {
                let output: Output = pipe.process(input).await;
                context.output(output).await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_channel::unbounded;

    #[tokio::test]
    async fn test_pipeable() {
        struct TestPipe;

        impl Pipeable<i32, i32> for TestPipe {
            async fn process(&self, input: i32) -> i32 {
                input * 2
            }
        }

        let pipe = Box::new(TestPipe);
        let output = pipe.process(5).await;

        assert_eq!(output, 10);
    }
}
