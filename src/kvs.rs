pub struct KvStore {

}

impl KvStore {
    pub fn new() -> KvStore {
        KvStore {}
    }

    pub fn set(&mut self, _key: String, _value: String) {
        unimplemented!("Nothing here")
    }

    pub fn get(&self, _key: String) -> Option<String> {
        unimplemented!("Nothing here")
    }

    pub fn remove(&mut self, _key: String) {
        unimplemented!("Nothing here")
    }
}
