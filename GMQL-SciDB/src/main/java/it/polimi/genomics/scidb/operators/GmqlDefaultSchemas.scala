package it.polimi.genomics.scidb.operators

import it.polimi.genomics.scidb.GmqlSciConfig
import it.polimi.genomics.scidbapi.SciStoredArray
import it.polimi.genomics.scidbapi.schema.DataType._
import it.polimi.genomics.scidbapi.schema.{Attribute, Dimension}

import scala.xml.XML


object GDS
{

  // ------------------------------------------------------------
  // -- GENERAL -------------------------------------------------

  val null1_A   = new Attribute("null1", INT64)
  val null2_A   = new Attribute("null2", INT64)

  val null1 = null1_A.e
  val null2 = null2_A.e

  // ------------------------------------------------------------
  // -- METADATA ------------------------------------------------

  val nid1_D  = Dimension("nid_1",0,None,1000,0)
  val nid2_D  = Dimension("nid_2",0,None,1000,0)
  val vid1_D  = Dimension("vid_1",0,None,1000,0)
  val vid2_D  = Dimension("vid_2",0,None,1000,0)
  val sid_D   = if(GmqlSciConfig.chuck_mode) Dimension("sid", 0, None, 1, 0) else Dimension("sid", 0, None, 1000, Dimension.DEFAULT_OVERLAP)

  val name_A  = Attribute("name", STRING)
  val value_A = Attribute("value", STRING)

  val meta_dimensions_D = List(nid1_D, nid2_D, vid1_D, vid2_D, sid_D)
  val meta_attributes_A = List(name_A, value_A)

  // ------------------------------------------------------------
  // -- REGION --------------------------------------------------

  val _chr_chunk_size = 1

  val chri_D    = Dimension("i", 0, None, 1, Dimension.DEFAULT_OVERLAP)

  val chunk_size = 1000000

  val chr_D     = if(GmqlSciConfig.chuck_mode) Dimension("chr", 0, None, _chr_chunk_size, 0) else Dimension("chr", 0, None, _chr_chunk_size, Dimension.DEFAULT_OVERLAP)
  val left_D    = if(GmqlSciConfig.chuck_mode) Dimension("left", 0, None, chunk_size, 0) else Dimension("left", 0, None, 100, Dimension.DEFAULT_OVERLAP)
  val right_D   = if(GmqlSciConfig.chuck_mode) Dimension("right", 0, None, chunk_size, 0) else Dimension("right", 0, None, 100, Dimension.DEFAULT_OVERLAP)
  val strand_D  = if(GmqlSciConfig.chuck_mode) Dimension("strand", 0, Some(2), 3, 0) else Dimension("strand", 0, Some(2), 3, Dimension.DEFAULT_OVERLAP)
  val enumeration_D = if(GmqlSciConfig.chuck_mode) Dimension("x", 0, None, 1000000, 1) else Dimension("x", 0, None, 100, 1)

  val coordinates_dimensions_D = List(chr_D, left_D, right_D, strand_D)
  val coordinates_dimensions_A = List(chr_D.toAttribute(false), left_D.toAttribute(false), right_D.toAttribute(false), strand_D.toAttribute(false))

  val regions_dimensions_D = List(sid_D, chr_D, left_D, right_D, strand_D, enumeration_D)

  // ------------------------------------------------------------
  // -- METAJOIN ------------------------------------------------

  val sid_ANCHOR_D      = sid_D.label("ANCHOR")
  val sid_EXPERIMENT_D  = sid_D.label("EXPERIMENT")
  val sid_RESULT_A      = sid_D.label("RESULT").toAttribute()

  val metajoin_dimensions_D = List(sid_ANCHOR_D, sid_EXPERIMENT_D)
  val metajoin_attributes_A = List(sid_RESULT_A)

  // ------------------------------------------------------------
  // -- METAGROUP -----------------------------------------------

  val gid_D     = sid_D.rename("gid")
  val gid_A     = gid_D.toAttribute()

  val metagroup_dimensions_D = List(sid_D, gid_D)
  val metagroup_attributes_A = List(null1_A)

  // ------------------------------------------------------------
  // -- IO ------------------------------------------------------

  val CHR_A     = new Attribute("CHR", STRING, false)
  val LEFT_A    = new Attribute("LEFT", INT64)
  val RIGHT_A   = new Attribute("RIGHT", INT64)
  val STRAND_A  = new Attribute("STRAND", STRING)

  val left_A    = left_D.toAttribute()
  val right_A   = right_D.toAttribute()
  val strand_A  = strand_D.toAttribute()

  val import_dimensions_A = List(CHR_A, left_A, right_A, STRAND_A)
  val export_dimensions_A = List(CHR_A, LEFT_A, RIGHT_A, STRAND_A)

  // ------------------------------------------------------------
  // ------------------------------------------------------------

  val offset_rep = 8;

  val offset_D  = new Dimension("offset",0,Some(offset_rep-1),2,0)

  val RD_CHR = new SciStoredArray(List(GDS.chri_D), List(GDS.CHR_A), "RD_CHR", "RD_CHR")
  val RD_OFFSET = new SciStoredArray(List(offset_D), List(new Attribute("f", BOOL)), "RD_OFFSET_"+offset_rep, "RD_OFFSET_"+offset_rep)

}
