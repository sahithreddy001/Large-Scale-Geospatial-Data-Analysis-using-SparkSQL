package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    if(queryRectangle == null || pointString == null || queryRectangle.isEmpty() || pointString.isEmpty())
      return false
    var kavalsina_rectanlge = queryRectangle.split(",")
    var x_coordinate_1 = kavalsina_rectanlge(0).trim.toDouble
    var y_coordinate_1 = kavalsina_rectanlge(1).trim.toDouble
    var x_coordinate_2 = kavalsina_rectanlge(2).trim.toDouble
    var y_coordinate_2 = kavalsina_rectanlge(3).trim.toDouble

    var kavalsina_point = pointString.split(",")
    var x_coordinate_p = kavalsina_point(0).trim.toDouble
    var y_coordinate_p = kavalsina_point(1).trim.toDouble

    var mn_x_coordinate_1_2 = math.min(x_coordinate_1, x_coordinate_2)
    var mx_x_coordinate_1_2 = math.max(x_coordinate_1, x_coordinate_2)
    var mn_y_coordinate_1_2 = math.min(y_coordinate_1, y_coordinate_2)
    var mx_y_coordinate_1_2 = math.max(y_coordinate_1, y_coordinate_2)

    if(x_coordinate_p >= mn_x_coordinate_1_2 && x_coordinate_p <= mx_x_coordinate_1_2 && y_coordinate_p >= mn_y_coordinate_1_2 && y_coordinate_p <= mx_y_coordinate_1_2){
      return true
    }
    return false
  }
}