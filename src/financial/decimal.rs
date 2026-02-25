use serde::{Serialize, Deserialize, Serializer, Deserializer};
use std::ops::{Add, Sub, Mul, Div, AddAssign, SubAssign};
use std::fmt;
use std::str::FromStr;

/// Fixed-point decimal number with 18 digits of precision
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Decimal {
    // Value * 10^18
    raw: i128,
}

const SCALE: i128 = 1_000_000_000_000_000_000; // 10^18

impl Decimal {
    pub fn new(value: i128) -> Self {
        Self { raw: value }
    }

    pub fn from_f64(value: f64) -> Self {
        let scaled = (value * SCALE as f64) as i128;
        Self { raw: scaled }
    }
    
    pub fn to_f64(&self) -> f64 {
        self.raw as f64 / SCALE as f64
    }

    pub fn zero() -> Self {
        Self { raw: 0 }
    }
    
    pub fn abs(&self) -> Self {
        Self { raw: self.raw.abs() }
    }
}

impl Add for Decimal {
    type Output = Self;
    fn add(self, rhs: Self) -> Self {
        Self { raw: self.raw + rhs.raw }
    }
}

impl Sub for Decimal {
    type Output = Self;
    fn sub(self, rhs: Self) -> Self {
        Self { raw: self.raw - rhs.raw }
    }
}

impl Mul for Decimal {
    type Output = Self;
    fn mul(self, rhs: Self) -> Self {
        // Multiply then divide by scale
        let product = (self.raw as i128 * rhs.raw as i128) / SCALE;
        Self { raw: product }
    }
}

impl AddAssign for Decimal {
    fn add_assign(&mut self, rhs: Self) {
        self.raw += rhs.raw;
    }
}

impl SubAssign for Decimal {
    fn sub_assign(&mut self, rhs: Self) {
        self.raw -= rhs.raw;
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let int_part = self.raw / SCALE;
        let frac_part = (self.raw % SCALE).abs();
        write!(f, "{}.{:018}", int_part, frac_part)
    }
}

// Serde integration
impl Serialize for Decimal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Decimal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let s = String::deserialize(deserializer)?;
        let parts: Vec<&str> = s.split('.').collect();
        if parts.is_empty() { return Ok(Decimal::zero()); }
        
        let int_part: i128 = parts[0].parse().map_err(serde::de::Error::custom)?;
        let mut frac_part: i128 = 0;
        
        if parts.len() > 1 {
            let frac_str = parts[1];
            let len = frac_str.len();
            let parsed: i128 = frac_str.parse().map_err(serde::de::Error::custom)?;
            // Adjust to 18 digits
            if len < 18 {
                frac_part = parsed * 10_i128.pow(18 - len as u32);
            } else {
                let diff = len - 18;
                frac_part = parsed / 10_i128.pow(diff as u32);
            }
        }
        
        if int_part < 0 {
            Ok(Decimal { raw: int_part * SCALE - frac_part })
        } else {
            Ok(Decimal { raw: int_part * SCALE + frac_part })
        }
    }
}

impl Default for Decimal {
    fn default() -> Self { Self::zero() }
}
