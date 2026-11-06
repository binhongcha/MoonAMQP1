///|
/// Some(Null) stores AMQP null; None removes the named application property.
pub fn Message::with_application_property(
  self : Message,
  name : String,
  value : Value?,
  limits~ : Limits = Limits::default(),
) -> Message raise AmqpError {
  let entries : Array[(Value, Value)] = []
  for s in self.sections {
    if s.descriptor == 0x74 && s.body is MapValue(items) {
      for (key, old) in items {
        let remove = match key {
          Text(k) => k == name
          _ => false
        }
        if !remove {
          entries.push((key, old))
        }
      }
    }
  }
  if value is Some(v) {
    entries.push((Text(name), v))
  }
  ignore(text_bytes(name, maximum=limits.max_value_bytes))
  let sections : Array[Section] = []
  let mut inserted = false
  for s in self.sections {
    if !inserted && s.descriptor >= 0x74 {
      if !entries.is_empty() {
        sections.push({ descriptor: 0x74, body: MapValue(entries), })
      }
      inserted = true
    }
    if s.descriptor != 0x74 {
      sections.push(s)
    }
  }
  if !inserted && !entries.is_empty() {
    sections.push({ descriptor: 0x74, body: MapValue(entries), })
  }
  Message::new(sections, limits~)
}
