package com.siemens.gbs.analytics

object Test extends App{
   val appName="tape-logdelivery-test-jr_e0dd661f15bef69dd1dd9320a53792726141e2260d947cee7713e1910eab1f67"
   val matchr=appName.substring(5)
   val (jobName,jobrunID)=(matchr.split("-jr")(0) , "jr"+matchr.split("-jr")(1))
   println(jobName)
   println(jobrunID)
}