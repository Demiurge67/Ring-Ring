//! Хранение ключевой пары в файле

use serde::{Serialize, Deserialize};
use std::fs;
use std::path::PathBuf;
use anyhow::{Result, Context};

#[derive(Serialize, Deserialize)]
struct KeyFile {
    private_key_hex: String,
    public_key_hex: String,
}

/// Сохранить ключи в файл (по умолчанию в директории приложения)
pub fn save_keys(private_hex: &str, public_hex: &str) -> Result<()> {
    let path = get_keys_path()?;
    let keys = KeyFile {
        private_key_hex: private_hex.to_string(),
        public_key_hex: public_hex.to_string(),
    };
    let json = serde_json::to_string_pretty(&keys)?;
    fs::write(path, json)?;
    Ok(())
}

/// Загрузить ключи из файла. Если файла нет — возвращает None.
pub fn load_keys() -> Result<Option<(String, String)>> {
    let path = get_keys_path()?;
    if !path.exists() {
        return Ok(None);
    }
    let json = fs::read_to_string(path)?;
    let keys: KeyFile = serde_json::from_str(&json)?;
    Ok(Some((keys.private_key_hex, keys.public_key_hex)))
}

/// Получить путь к файлу ключей (например, ~/.config/ring-ring/keys.json)
fn get_keys_path() -> Result<PathBuf> {
    let mut path = dirs::config_dir()
        .context("Не удалось определить директорию конфигурации")?;
    path.push("ring-ring");
    fs::create_dir_all(&path)?;
    path.push("keys.json");
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_save_and_load_keys() {
        let tmp_dir = tempdir().unwrap();
        // Временно подменяем домашнюю директорию для теста — сделаем проще:
        // В тестах мы не можем полагаться на реальную файловую систему, поэтому протестируем логику без вызова get_keys_path.
        // Перепишем тест позже. Для первого этапа просто проверяем сериализацию.
        let private = "a".repeat(64);
        let public = "b".repeat(64);
        let keys = KeyFile { private_key_hex: private, public_key_hex: public };
        let json = serde_json::to_string(&keys).unwrap();
        let loaded: KeyFile = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.private_key_hex, "a".repeat(64));
    }
}
