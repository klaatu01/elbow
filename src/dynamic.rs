use crate::{Context, Pipe};
use std::any::Any;

pub struct DynPipeAdapter<P, I, O>
where
    P: Pipe<I, O> + Send + Sync + 'static,
    I: Any + Send + Sync + 'static,
    O: Any + Send + Sync + 'static,
{
    pipe: P,
    _phantom: std::marker::PhantomData<(I, O)>,
}

impl<P, I, O> DynPipeAdapter<P, I, O>
where
    P: Pipe<I, O> + Send + Sync + 'static,
    I: Any + Send + Sync + 'static,
    O: Any + Send + Sync + 'static,
{
    pub fn new(pipe: P) -> Self {
        DynPipeAdapter {
            pipe,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn into_dyn(self) -> Box<dyn DynPipe> {
        Box::new(self)
    }
}

pub trait DynPipe: Send + Sync + 'static {
    fn run_box(self: Box<Self>, ctx: Context);
}

impl<P, I, O> DynPipe for DynPipeAdapter<P, I, O>
where
    P: Pipe<I, O> + Send + Sync + 'static,
    I: Any + Send + Sync + 'static,
    O: Any + Send + Sync + 'static,
{
    fn run_box(self: Box<Self>, ctx: Context) {
        Box::new(self.pipe).run(ctx);
    }
}
