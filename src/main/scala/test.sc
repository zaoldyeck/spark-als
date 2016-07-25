val s: String = ""
s.orElse("abc")
val l: List[Double] = List(1, 6, 7, 8, 4, 6, 5, 3, 1, 100)
val max = l.max
val min = l.min
val r = max - min
val av = l.sum / l.length.toDouble

val fs = l.map(v => (v - av) / r)
fs.map(v => v - fs.min)