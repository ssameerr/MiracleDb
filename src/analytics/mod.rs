pub struct HyperLogLog {
    registers: Vec<u8>,
    m: usize,
}

impl HyperLogLog {
    pub fn new(b: u8) -> Self {
        let m = 1 << b;
        Self {
            registers: vec![0; m],
            m,
        }
    }
    
    pub fn add(&mut self, item: &[u8]) {
        // Stub implementation
        // Hash item, update register
        // registers[idx] = max(registers[idx], rho)
    }
    
    pub fn count(&self) -> f64 {
        // Harmonic mean
        0.0
    }
}
