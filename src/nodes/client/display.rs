use std::collections::HashMap;

use anna_api::{
    lattice::{
        causal::SingleKeyCausalLattice, LastWriterWinsLattice, Lattice, MapLattice, MaxLattice,
        SetLattice,
    },
    LatticeValue,
};
use eyre::Context;

pub(crate) trait LatticeDisplay {
    fn display(&self) -> eyre::Result<String>;
}

impl LatticeDisplay for LatticeValue {
    fn display(&self) -> eyre::Result<String> {
        match self {
            LatticeValue::Lww(v) => Ok(format!("Lww:\n{}", v.display()?)),
            LatticeValue::Set(set) => Ok(format!("Set:\n{}", set.display()?)),
            LatticeValue::OrderedSet(set) => Ok(format!("OrderedSet:\n{set:#?}")),
            LatticeValue::SingleCausal(set) => Ok(format!("CausalSet:\n{}", set.display()?)),
            LatticeValue::SingleCausalMap(map) => Ok(format!("CausalMap:\n{}", map.display()?)),
            LatticeValue::MultiCausal(set) => Ok(format!("MultiCausal:\n{set:#?}")),
        }
    }
}

impl LatticeDisplay for LastWriterWinsLattice<Vec<u8>> {
    fn display(&self) -> eyre::Result<String> {
        let value = self.element().value();
        String::from_utf8(value.clone()).context("value is not valid utf8")
    }
}

impl LatticeDisplay for SetLattice<Vec<u8>> {
    fn display(&self) -> eyre::Result<String> {
        let mut set: Vec<_> = self
            .reveal()
            .iter()
            .map(|v| std::str::from_utf8(v))
            .collect::<Result<_, _>>()
            .context("received value is not valid utf8")?;
        set.sort_unstable();

        Ok(format!("{set:?}"))
    }
}

impl<V: LatticeDisplay + Lattice + Clone> LatticeDisplay for MapLattice<String, V> {
    fn display(&self) -> eyre::Result<String> {
        let map: HashMap<String, String> = self
            .reveal()
            .iter()
            .map(|(k, v)| v.display().map(|ok| (k.clone(), ok)))
            .collect::<Result<_, _>>()
            .context("received value is not valid utf8")?;

        Ok(format!("{map:#?}"))
    }
}

impl LatticeDisplay for MaxLattice<usize> {
    fn display(&self) -> eyre::Result<String> {
        Ok(format!("{}", self.reveal()))
    }
}

impl<T: LatticeDisplay + Lattice + Clone> LatticeDisplay for SingleKeyCausalLattice<T> {
    fn display(&self) -> eyre::Result<String> {
        let clock = self.reveal().vector_clock.display()?;
        let value = self.reveal().value.display()?;
        Ok(format!("(\nvector_clock: {clock}\nvalue: {value}\n)"))
    }
}
