package chrism.sdsc.model

final case class WordFrequency(word: String, frequency: Long = 1L) {

  def +(that: WordFrequency): WordFrequency = {
    require(word == that.word, s"The words do not match: $word vs. ${that.word}")

    WordFrequency(word, frequency + that.frequency)
  }
}
