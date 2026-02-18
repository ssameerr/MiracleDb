//! FHIR R4 resource types and operations for healthcare data management

use serde::{Serialize, Deserialize};
use std::collections::HashMap;

/// FHIR Resource types
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum FhirResourceType {
    Patient,
    Observation,
    MedicationRequest,
    Condition,
    Encounter,
    Procedure,
    DiagnosticReport,
    AllergyIntolerance,
    Immunization,
    CarePlan,
}

/// FHIR CodeableConcept
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CodeableConcept {
    pub coding: Vec<Coding>,
    pub text: Option<String>,
}

/// FHIR Coding
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Coding {
    pub system: Option<String>,
    pub code: Option<String>,
    pub display: Option<String>,
}

/// FHIR Reference
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Reference {
    pub reference: String,
    pub display: Option<String>,
}

/// FHIR Observation resource
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Observation {
    pub id: String,
    pub status: String,  // registered | preliminary | final | amended
    pub code: CodeableConcept,
    pub subject: Reference,
    pub effective_date_time: Option<String>,
    pub value_quantity: Option<Quantity>,
    pub interpretation: Vec<CodeableConcept>,
}

/// FHIR Quantity
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Quantity {
    pub value: f64,
    pub unit: String,
    pub system: Option<String>,
    pub code: Option<String>,
}

/// HL7 message segment types
#[derive(Clone, Debug)]
pub enum Hl7Segment {
    Msh {  // Message Header
        sending_app: String,
        sending_facility: String,
        message_type: String,
        message_id: String,
        timestamp: String,
    },
    Pid {  // Patient Identification
        patient_id: String,
        patient_name: String,
        date_of_birth: String,
        sex: String,
    },
    Obx {  // Observation/Result
        value_type: String,
        observation_id: String,
        observation_value: String,
        units: String,
        reference_range: Option<String>,
        abnormal_flag: Option<String>,
    },
    Pv1 {  // Patient Visit
        patient_class: String,
        assigned_location: String,
        admission_type: String,
    },
}

/// HL7 v2 message
#[derive(Clone, Debug)]
pub struct Hl7Message {
    pub segments: Vec<Hl7Segment>,
}

impl Hl7Message {
    /// Parse a basic HL7 v2 message
    pub fn parse(raw: &str) -> Result<Self, String> {
        let mut segments = Vec::new();

        for line in raw.lines() {
            let line = line.trim();
            if line.is_empty() { continue; }

            let parts: Vec<&str> = line.splitn(20, '|').collect();
            if parts.is_empty() { continue; }

            match parts[0] {
                "MSH" => {
                    if parts.len() >= 10 {
                        segments.push(Hl7Segment::Msh {
                            sending_app: parts.get(2).unwrap_or(&"").to_string(),
                            sending_facility: parts.get(3).unwrap_or(&"").to_string(),
                            message_type: parts.get(8).unwrap_or(&"").to_string(),
                            message_id: parts.get(9).unwrap_or(&"").to_string(),
                            timestamp: parts.get(6).unwrap_or(&"").to_string(),
                        });
                    }
                }
                "PID" => {
                    segments.push(Hl7Segment::Pid {
                        patient_id: parts.get(3).unwrap_or(&"").to_string(),
                        patient_name: parts.get(5).unwrap_or(&"").to_string(),
                        date_of_birth: parts.get(7).unwrap_or(&"").to_string(),
                        sex: parts.get(8).unwrap_or(&"").to_string(),
                    });
                }
                "OBX" => {
                    segments.push(Hl7Segment::Obx {
                        value_type: parts.get(2).unwrap_or(&"").to_string(),
                        observation_id: parts.get(3).unwrap_or(&"").to_string(),
                        observation_value: parts.get(5).unwrap_or(&"").to_string(),
                        units: parts.get(6).unwrap_or(&"").to_string(),
                        reference_range: parts.get(7).map(|s| s.to_string()).filter(|s| !s.is_empty()),
                        abnormal_flag: parts.get(8).map(|s| s.to_string()).filter(|s| !s.is_empty()),
                    });
                }
                "PV1" => {
                    segments.push(Hl7Segment::Pv1 {
                        patient_class: parts.get(2).unwrap_or(&"").to_string(),
                        assigned_location: parts.get(3).unwrap_or(&"").to_string(),
                        admission_type: parts.get(4).unwrap_or(&"").to_string(),
                    });
                }
                _ => {} // Skip unknown segments
            }
        }

        if segments.is_empty() {
            return Err("No valid HL7 segments found".to_string());
        }

        Ok(Hl7Message { segments })
    }

    /// Convert HL7 message to FHIR observations
    pub fn to_fhir_observations(&self, patient_id: &str) -> Vec<Observation> {
        let mut obs = Vec::new();

        for segment in &self.segments {
            if let Hl7Segment::Obx { value_type: _, observation_id, observation_value, units, reference_range: _, abnormal_flag } = segment {
                // Parse numeric value
                let value_quantity = observation_value.parse::<f64>().ok().map(|v| Quantity {
                    value: v,
                    unit: units.clone(),
                    system: Some("http://unitsofmeasure.org".to_string()),
                    code: Some(units.clone()),
                });

                let interpretation = abnormal_flag.as_ref().map(|flag| vec![CodeableConcept {
                    coding: vec![Coding {
                        system: Some("http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation".to_string()),
                        code: Some(flag.clone()),
                        display: None,
                    }],
                    text: Some(flag.clone()),
                }]).unwrap_or_default();

                obs.push(Observation {
                    id: format!("obs-{}", obs.len() + 1),
                    status: "final".to_string(),
                    code: CodeableConcept {
                        coding: vec![Coding {
                            system: None,
                            code: Some(observation_id.clone()),
                            display: Some(observation_id.clone()),
                        }],
                        text: Some(observation_id.clone()),
                    },
                    subject: Reference {
                        reference: format!("Patient/{}", patient_id),
                        display: None,
                    },
                    effective_date_time: None,
                    value_quantity,
                    interpretation,
                });
            }
        }

        obs
    }
}

/// FHIR Resource Store
pub struct FhirResourceStore {
    observations: HashMap<String, Observation>,
}

impl FhirResourceStore {
    pub fn new() -> Self {
        Self { observations: HashMap::new() }
    }

    pub fn store_observation(&mut self, obs: Observation) {
        self.observations.insert(obs.id.clone(), obs);
    }

    pub fn get_observation(&self, id: &str) -> Option<&Observation> {
        self.observations.get(id)
    }

    pub fn patient_observations(&self, patient_id: &str) -> Vec<&Observation> {
        self.observations.values()
            .filter(|o| o.subject.reference.ends_with(patient_id))
            .collect()
    }
}

impl Default for FhirResourceStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hl7_parse() {
        let msg = "MSH|^~\\&|LAB|HOSP||HIS|20240118120000||ORU^R01|MSG001|P|2.5\nPID|||P12345||Doe^John||19800101|M\nOBX|1|NM|8480-6^Glucose||95|mg/dL|70-99|N";
        let parsed = Hl7Message::parse(msg).unwrap();
        assert!(!parsed.segments.is_empty());
    }

    #[test]
    fn test_hl7_to_fhir() {
        let msg = "MSH|^~\\&|LAB|HOSP\nPID|||P001||Test^Patient\nOBX|1|NM|8480-6||120|mmHg|90-140|N";
        let parsed = Hl7Message::parse(msg).unwrap();
        let obs = parsed.to_fhir_observations("P001");
        assert!(!obs.is_empty());
        assert_eq!(obs[0].value_quantity.as_ref().unwrap().value, 120.0);
    }
}
