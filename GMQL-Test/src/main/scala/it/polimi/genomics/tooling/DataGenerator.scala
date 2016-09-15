package it.polimi.genomics.tooling

/**
 * Generates a random dataset for testing
 */
object DataGenerator {

  /**
   * @param number_of_couples maximum number of couples. Since it removes duplicates, the
   *                          actual number of couples in the metadata may be less than
   *                          the parameter value.
   * @return a list of attribute values pare, with no duplicates
   */
  def generateMetaData(number_of_couples : Int, fileNumber : Int = 0) : List[String] = {

    (for (i <- 1 to number_of_couples) yield {
      getRandomMetaTuple()
    }).distinct.toList ++ List("File_Number\t" + fileNumber)
  }

  def getRandomMetaTuple() : String = {

    val random = scala.util.Random.nextInt(5)

    def getRandomAntibodyValue() = {
      val randomAntibody = scala.util.Random.nextInt(3)
      randomAntibody match {
        case 0 => "CTCF"
        case 1 => "BRD4"
        case 2 => "GTF2i"
      }
    }

    def getRandomOrganismValue() = {
      val randomAntibody = scala.util.Random.nextInt(3)
      randomAntibody match {
        case 0 => "hg19"
        case 1 => "hg18"
        case 2 => "mm9"
      }
    }

    def getRandomTypeValue() = {
      val randomAntibody = scala.util.Random.nextInt(3)
      randomAntibody match {
        case 0 => "chipseq"
        case 1 => "rnaseq"
        case 2 => "chiapet"
      }
    }

    def getRandomCellValue() = {
      val randomAntibody = scala.util.Random.nextInt(4)
      randomAntibody match {
        case 0 => "K562"
        case 1 => "hela-s3"
        case 2 => "h1"
        case 3 => "gliobla"
      }
    }

    def getRandomAverageValue() = {
      val randomAverage = scala.util.Random.nextInt(3)
      randomAverage match {
        case 0 => "0.0"
        case 1 => "0.5"
        case 2 => "1.0"
      }

    }

    random match {
      case 0 => "antibody\t" + getRandomAntibodyValue()
      case 1 => "organism\t" + getRandomOrganismValue()
      case 2 => "type\t" + getRandomTypeValue()
      case 3 => "cell\t" + getRandomCellValue()
      case 4 => "average\t" + getRandomAverageValue()
    }

  }

  /**
   * Computes a list of regions coordinates
   *
   * @param number_of_chr number of chrmosomes
   * @param regions_per_chr number of regions per chromosome
   * @param chr_len maximum poisition for the start point of a region in a chromosome
   * @param min_region_len minimum length of each region
   * @param max_region_len maximum length of each region
   * @param strands list of available strands
   * @return a list of coordinates (no value is present here)
   */
  def generateRegionData(number_of_chr : Int, regions_per_chr : Int, chr_len : Int,
                         min_region_len : Int, max_region_len : Int,
                         strands : List[String]) : List[String] = {

    var regions:List[String] = List.empty

    for (chr <- 1 to number_of_chr) {
      val chr_regions = for (i <- 1 to regions_per_chr) yield {
        val start = scala.util.Random.nextInt(chr_len)
        val stop = start + scala.util.Random.nextInt(max_region_len - min_region_len) + min_region_len
        val strand = if(strands.nonEmpty) Some(strands(scala.util.Random.nextInt(strands.size))) else None
        "chr" + chr + "\t" + start + "\t" + stop + (if (strand.isDefined) ("\t" + strand.get) else "")
      }
      regions = regions ++ chr_regions
    }
    regions
  }


  def add_score_value(regions : List[String], min : Double, max : Double) : List[String] = {
    regions.map {x => x+ "\t" + (scala.util.Random.nextDouble()*max + min)}
  }

  def add_name_value(regions : List[String], names : List[String]) : List[String] = {
    regions.map {x => x+ "\t" + names(scala.util.Random.nextInt(names.size))}
  }


}
