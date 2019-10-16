package chapter08

import com.esri.core.geometry.{Geometry, GeometryEngine, SpatialReference}

/**
  * Taxi trip data from:
  * http://eeyore.ucdavis.edu/stat242/Homeworks/hw5.html
  *
  * @param geometry
  * @param spatialReference
  */
class RichGeometry(val geometry: Geometry,
                   val spatialReference: SpatialReference = SpatialReference.create(4326)) {

  def area2D(): Double = geometry.calculateArea2D()


  def contains(other: Geometry): Boolean = {
    GeometryEngine.contains(geometry, other, spatialReference)
  }

  def distance(other: Geometry): Double = {
    GeometryEngine.distance(geometry, other, spatialReference)
  }
}

object RichGeometry {

  implicit def wrapGeometry(g: Geometry): RichGeometry = {
    new RichGeometry(g)
  }
}