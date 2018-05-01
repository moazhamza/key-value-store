typedef i16 Key
typedef i32 Timestamp
typedef string Value

exception SystemException {
  1: optional string message
}

enum ConsistencyLevel{
    ONE,
    QUORUM
}

struct GetResult{
   1: bool success,
   2: Value result,
   3: Timestamp time
}


service KeyValueStore {
  Value get(1: Key key, 2: ConsistencyLevel lvl)
    throws (1: SystemException systemException),

  GetResult get_aux(1: Key key)
    throws (1: SystemException systemException),

  bool put(1: Key key, 2: Value val, 3: ConsistencyLevel lvl)
    throws (1: SystemException systemException),

  bool put_aux(1: Key key, 2: Value val, 3: Timestamp ts)
    throws (1: SystemException systemException)
}