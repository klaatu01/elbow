mod pipeable;
use pipeable::{Context, Pipeable};
use std::any::Any;

pub struct PipelineMap<I, O, F>
where
    I: Any + Send + Sync + 'static,
    O: Any + Send + Sync + 'static,
    F: Fn(I) -> O + Send + Sync + 'static,
{
    map_fn: F,
    _phantom: std::marker::PhantomData<(I, O)>,
}

impl<I, O, F> Pipeable<I, O> for PipelineMap<I, O, F>
where
    I: Any + Send + Sync + 'static,
    O: Any + Send + Sync + 'static,
    F: Fn(I) -> O + Send + Sync + 'static,
{
    async fn process(&self, input: I) -> O {
        (self.map_fn)(input)
    }
}

pub struct DynAdapter<P, I, O>
where
    P: Pipeable<I, O> + Send + Sync + 'static,
    I: Any + Send + Sync + 'static,
    O: Any + Send + Sync + 'static,
{
    pipe: P,
    _phantom: std::marker::PhantomData<(I, O)>,
}

impl<P, I, O> DynAdapter<P, I, O>
where
    P: Pipeable<I, O> + Send + Sync + 'static,
    I: Any + Send + Sync + 'static,
    O: Any + Send + Sync + 'static,
{
    pub fn new(pipe: P) -> Self {
        DynAdapter {
            pipe,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn as_dyn(self) -> Box<dyn DynPipeline> {
        Box::new(self)
    }
}

pub trait DynPipeline: Send + Sync + 'static {
    fn run_box(self: Box<Self>, ctx: Context);
}

impl<P, I, O> DynPipeline for DynAdapter<P, I, O>
where
    P: Pipeable<I, O> + Send + Sync + 'static,
    I: Any + Send + Sync + 'static,
    O: Any + Send + Sync + 'static,
{
    fn run_box(self: Box<Self>, ctx: Context) {
        tokio::spawn(async move {
            while let Some(input) = ctx.input::<I>().await {
                let output: O = self.pipe.process(input).await;
                ctx.output(output).await;
            }
        });
    }
}

pub struct PipelineBuilder;

impl PipelineBuilder {
    pub fn first<I, O>(pipe: impl Pipeable<I, O> + 'static) -> PipelineBuilderThen<I, I, O>
    where
        I: Any + Send + Sync + 'static,
        O: Any + Send + Sync + 'static,
    {
        let dyn_pipe: Box<dyn DynPipeline> = DynAdapter::new(pipe).as_dyn();
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
    pipes: Vec<Box<dyn DynPipeline>>,
    _phantom: std::marker::PhantomData<(Input, NextInput, Output)>,
}

impl<Input, NextInput, Output> PipelineBuilderThen<Input, NextInput, Output>
where
    Input: Any + Send + Sync + 'static,
    NextInput: Any + Send + Sync + 'static,
    Output: Any + Send + Sync + 'static,
{
    pub fn new(pipe: impl Pipeable<Input, NextInput> + 'static) -> Self {
        let dyn_pipe: Box<dyn DynPipeline> = DynAdapter::new(pipe).as_dyn();
        PipelineBuilderThen {
            pipes: vec![dyn_pipe],
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn then<P, O2>(self, pipe: P) -> PipelineBuilderThen<Input, Output, O2>
    where
        P: Pipeable<Output, O2> + Send + Sync + 'static,
        O2: Any + Send + Sync + 'static,
    {
        let dyn_pipe: Box<dyn DynPipeline> = DynAdapter::new(pipe).as_dyn();
        let mut pipes = self.pipes;
        pipes.push(dyn_pipe);
        PipelineBuilderThen {
            pipes,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn map<F, O2>(self, map_fn: F) -> PipelineBuilderThen<Input, Output, O2>
    where
        F: Fn(Output) -> O2 + Send + Sync + 'static,
        O2: Any + Send + Sync + 'static,
    {
        let map_pipe = PipelineMap {
            map_fn,
            _phantom: std::marker::PhantomData,
        };

        let dyn_pipe: Box<dyn DynPipeline> = DynAdapter::new(map_pipe).as_dyn();
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

        // 1) allocate N+1 channels
        for _ in 0..=n {
            let (tx, rx) = async_channel::unbounded::<Box<dyn Any + Send + Sync>>();
            txs.push(tx);
            rxs.push(rx);
        }

        // 2) wire each pipe: channel[i] → channel[i+1]
        for (i, pipe) in self.pipes.into_iter().enumerate() {
            let ctx = Context::new(rxs[i].clone(), txs[i + 1].clone());
            pipe.run_box(ctx);
        }

        // 3) return (final‐receiver, first‐sender)
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
}

pub struct Pipeline;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_different_types() {
        struct A;
        struct B;
        struct C;
        struct APipe;
        struct BPipe;

        impl Pipeable<A, B> for APipe {
            async fn process(&self, _: A) -> B {
                B
            }
        }

        impl Pipeable<B, C> for BPipe {
            async fn process(&self, _: B) -> C {
                C
            }
        }

        let (input, output) = PipelineBuilder::first(APipe).then(BPipe).build();

        output.send(A).await.unwrap();
        let result = input.next().await;
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn complex_pipeline() {
        pub struct Person {
            name: String,
        }

        pub struct PersonWithAge {
            name: String,
            age: u32,
        }

        pub struct PersonWithAgeAndAddress {
            name: String,
            age: u32,
            address: String,
        }

        pub struct NameToPersonPipe;

        impl Pipeable<String, Person> for NameToPersonPipe {
            async fn process(&self, name: String) -> Person {
                Person { name }
            }
        }

        pub struct PersonToPersonWithAgePipe;
        impl Pipeable<Person, PersonWithAge> for PersonToPersonWithAgePipe {
            async fn process(&self, person: Person) -> PersonWithAge {
                PersonWithAge {
                    name: person.name,
                    age: 30, // hardcoded for simplicity
                }
            }
        }

        pub struct PersonWithAgeToPersonWithAgeAndAddressPipe;
        impl Pipeable<PersonWithAge, PersonWithAgeAndAddress>
            for PersonWithAgeToPersonWithAgeAndAddressPipe
        {
            async fn process(&self, person: PersonWithAge) -> PersonWithAgeAndAddress {
                PersonWithAgeAndAddress {
                    name: person.name,
                    age: person.age,
                    address: "123 Main St".to_string(), // hardcoded for simplicity
                }
            }
        }

        let (input, output) = PipelineBuilder::first(NameToPersonPipe)
            .map(|person: Person| PersonWithAge {
                name: person.name,
                age: 30,
            })
            .then(PersonWithAgeToPersonWithAgeAndAddressPipe)
            .build();

        output.send("Alice".to_string()).await.unwrap();
        let result = input.next().await;
        assert!(result.is_some());
        if let Some(person_with_address) = result {
            assert_eq!(person_with_address.name, "Alice");
            assert_eq!(person_with_address.age, 30);
            assert_eq!(person_with_address.address, "123 Main St");
        } else {
            panic!("Expected a PersonWithAgeAndAddress but got None");
        }
    }
}
