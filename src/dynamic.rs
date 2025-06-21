use crate::{Context, Pipeable};
use std::any::Any;

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

    pub fn into_dyn(self) -> Box<dyn DynPipeline> {
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
        Box::new(self.pipe).run(ctx);
    }
}
