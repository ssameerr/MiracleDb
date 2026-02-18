use serde::{Serialize, Deserialize};

pub mod dicom;
pub mod fhir;
pub use fhir::{FhirResourceStore, Hl7Message, Observation};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Patient {
    pub id: String,
    pub name: String,
    pub dob: String,
    pub active: bool,
}

pub struct FhirStore {
    patients: std::collections::HashMap<String, Patient>,
}

impl FhirStore {
    pub fn new() -> Self {
        Self { patients: std::collections::HashMap::new() }
    }

    pub fn add_patient(&mut self, p: Patient) {
        self.patients.insert(p.id.clone(), p);
    }

    pub fn get_patient(&self, id: &str) -> Option<&Patient> {
        self.patients.get(id)
    }

    // HIPAA Erase
    pub fn erase_patient(&mut self, id: &str) {
        self.patients.remove(id);
        // In reality, secure wipe from disk
    }
}
