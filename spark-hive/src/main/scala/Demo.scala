object Demo {
  def main(args: Array[String]): Unit = {
    val arr:Array[Int]=new Array[Int](1);
    arr(0)=10;
    arr.map(a=>{
      print(a)
    })
    val brr:List[String]=List("hello")
    val crr=brr ++("world")
    crr.map(b=>{
      print(b)
    })
  }
}
