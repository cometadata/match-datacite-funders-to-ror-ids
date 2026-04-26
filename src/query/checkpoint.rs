use anyhow::Result;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

pub struct Checkpoint {
    path: PathBuf,
    processed: HashSet<String>,
}

impl Checkpoint {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            processed: HashSet::new(),
        }
    }

    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut processed = HashSet::new();

        if path.exists() {
            let file = File::open(&path)?;
            let reader = BufReader::new(file);
            for line in reader.lines() {
                let hash = line?;
                if !hash.is_empty() {
                    processed.insert(hash);
                }
            }
        }

        Ok(Self { path, processed })
    }

    pub fn mark_processed(&mut self, hash: &str) {
        self.processed.insert(hash.to_string());
    }

    pub fn is_processed(&self, hash: &str) -> bool {
        self.processed.contains(hash)
    }

    pub fn save(&self) -> Result<()> {
        let file = File::create(&self.path)?;
        let mut writer = BufWriter::new(file);
        for hash in &self.processed {
            writeln!(writer, "{}", hash)?;
        }
        writer.flush()?;
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.processed.len()
    }

    pub fn is_empty(&self) -> bool {
        self.processed.is_empty()
    }
}
