mod builder;
mod collection;
mod core;
mod dynamic;
pub use builder::{Pipeline, PipelineBuilder};
pub use collection::PipelineExt;
pub use core::{Context, Pipe};
use dynamic::{DynPipe, DynPipeAdapter};

#[cfg(test)]
mod tests {
    use crate::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_different_types() {
        struct A;
        struct B;
        struct C;
        struct APipe;
        struct BPipe;

        impl Pipe<A, B> for APipe {
            async fn process(&self, _: A) -> Option<B> {
                Some(B)
            }
        }

        impl Pipe<B, C> for BPipe {
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

        #[derive(Debug)]
        pub struct Person {
            name: String,
        }

        #[derive(Debug)]
        pub struct PersonWithAge {
            name: String,
            age: u32,
        }

        #[derive(Debug)]
        pub struct PersonWithAgeAndAddress {
            name: String,
            age: u32,
            address: String,
        }

        pub struct NameToPersonPipe;

        impl Pipe<String, Person> for NameToPersonPipe {
            async fn process(&self, name: String) -> Option<Person> {
                println!("Converting name to person: {}", name);
                Some(Person { name })
            }
        }

        #[derive(Clone)]
        pub struct PersonToPersonWithAgePipe;
        impl Pipe<Person, PersonWithAge> for PersonToPersonWithAgePipe {
            async fn process(&self, person: Person) -> Option<PersonWithAge> {
                println!("Processing person: {}", person.name);
                tokio::time::sleep(Duration::from_secs(1)).await; // Simulate some processing delay
                Some(PersonWithAge {
                    name: person.name,
                    age: 30, // hardcoded for simplicity
                })
            }
        }

        #[derive(Clone)]
        pub struct PersonWithAgeToPersonWithAgeAndAddressPipe;
        impl Pipe<PersonWithAge, PersonWithAgeAndAddress> for PersonWithAgeToPersonWithAgeAndAddressPipe {
            async fn process(&self, person: PersonWithAge) -> Option<PersonWithAgeAndAddress> {
                tokio::time::sleep(Duration::from_secs(1)).await; // Simulate some processing delay
                Some(PersonWithAgeAndAddress {
                    name: person.name,
                    age: person.age,
                    address: "123 Main St".to_string(), // hardcoded for simplicity
                })
            }
        }

        let (input, output) = PipelineBuilder::first(NameToPersonPipe)
            .tee(|p: &Person| {
                println!("{:?}", p);
            })
            .filter(|p: &Person| p.name.starts_with("1"))
            .tee(|p: &Person| {
                println!("{:?}", p);
            })
            .then(
                PersonToPersonWithAgePipe
                    .batch(10, Duration::from_millis(100))
                    .concurrent(4),
            )
            .tee(|p: &PersonWithAge| {
                println!("{:?}", p);
            })
            .then(
                PersonWithAgeToPersonWithAgeAndAddressPipe
                    .batch(10, Duration::from_secs(100))
                    .concurrent(4),
            )
            .tee(|p: &PersonWithAgeAndAddress| {
                println!("{:?}", p);
            })
            .build();

        for i in 0..100 {
            let name = format!("{}", i);
            output.send(name).await.unwrap();
        }
        drop(output); // Close the output channel to signal completion
        let mut results = Vec::new();
        for _ in 0..100 {
            if let Some(person_with_address) = input.next().await {
                results.push(person_with_address);
            } else {
                break;
            }
        }
        assert_eq!(results.len(), 11);
    }
}
