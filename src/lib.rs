mod collection;
mod dynamic;
mod pipeable;
use dynamic::{DynAdapter, DynPipeline};
use pipeable::{Context, Pipeable};
use std::any::Any;

pub struct PipelineMap<I, O, F>
where
    I: Any + Send + Sync + 'static,
    O: Any + Send + Sync + 'static,
    F: Fn(I) -> Option<O> + Send + Sync + 'static,
{
    map_fn: F,
    _phantom: std::marker::PhantomData<(I, O)>,
}

impl<I, O, F> Pipeable<I, O> for PipelineMap<I, O, F>
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

impl<I, F> Pipeable<I, I> for PipelineFilter<I, F>
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

pub struct PipelineBuilder;

impl PipelineBuilder {
    pub fn first<I, O>(pipe: impl Pipeable<I, O> + 'static) -> PipelineBuilderThen<I, I, O>
    where
        I: Any + Send + Sync + 'static,
        O: Any + Send + Sync + 'static,
    {
        let dyn_pipe: Box<dyn DynPipeline> = DynAdapter::new(pipe).into_dyn();
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
        let dyn_pipe: Box<dyn DynPipeline> = DynAdapter::new(pipe).into_dyn();
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
        let dyn_pipe: Box<dyn DynPipeline> = DynAdapter::new(pipe).into_dyn();
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

        let dyn_pipe: Box<dyn DynPipeline> = DynAdapter::new(map_pipe).into_dyn();
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
}

pub struct Pipeline;

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test_different_types() {
        struct A;
        struct B;
        struct C;
        struct APipe;
        struct BPipe;

        impl Pipeable<A, B> for APipe {
            async fn process(&self, _: A) -> Option<B> {
                Some(B)
            }
        }

        impl Pipeable<B, C> for BPipe {
            async fn process(&self, _: B) -> Option<C> {
                Some(C)
            }
        }

        let (input, output) = PipelineBuilder::first(APipe).then(BPipe).build();

        output.send(A).await.unwrap();
        let result = input.next().await;
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn complex_pipeline() {
        use collection::PipelineExt;

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
            async fn process(&self, name: String) -> Option<Person> {
                println!("Converting name to person: {}", name);
                Some(Person { name })
            }
        }

        #[derive(Clone)]
        pub struct PersonToPersonWithAgePipe;
        impl Pipeable<Person, PersonWithAge> for PersonToPersonWithAgePipe {
            async fn process(&self, person: Person) -> Option<PersonWithAge> {
                println!("Processing person: {}", person.name);
                Some(PersonWithAge {
                    name: person.name,
                    age: 30, // hardcoded for simplicity
                })
            }
        }

        #[derive(Clone)]
        pub struct PersonWithAgeToPersonWithAgeAndAddressPipe;
        impl Pipeable<PersonWithAge, PersonWithAgeAndAddress>
            for PersonWithAgeToPersonWithAgeAndAddressPipe
        {
            async fn process(&self, person: PersonWithAge) -> Option<PersonWithAgeAndAddress> {
                println!(
                    "Converting PersonWithAge to PersonWithAgeAndAddress: {}",
                    person.name
                );
                Some(PersonWithAgeAndAddress {
                    name: person.name,
                    age: person.age,
                    address: "123 Main St".to_string(), // hardcoded for simplicity
                })
            }
        }

        let (input, output) = PipelineBuilder::first(NameToPersonPipe)
            .then(PersonToPersonWithAgePipe.batch(1000, Duration::from_millis(100)))
            .then(
                PersonWithAgeToPersonWithAgeAndAddressPipe
                    .batch(1, Duration::from_secs(10))
                    .concurrent(2),
            )
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
