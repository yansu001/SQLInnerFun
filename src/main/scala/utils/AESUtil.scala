package utils

import java.security.SecureRandom

import javax.crypto.{Cipher, KeyGenerator}
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger

object AESUtil {
  private val logger = Logger.getLogger(AESUtil.getClass)
  private val defaultCharset = "UTF-8"
  private val KEY_AES = "AES"
  private val KEY = "123456"
  /**
    * 加密
    *
    * @param data 需要加密的内容
    * @param key  加密密码
    * @return
    */
  def encrypt(data: String, key: String): String = doAES(data, key, Cipher.ENCRYPT_MODE)

  /**
    * 解密
    *
    * @param data 待解密内容
    * @param key  解密密钥
    * @return
    */
  def decrypt(data: String, key: String): String = doAES(data, key, Cipher.DECRYPT_MODE)

  /**
    * 加解密
    *
    * @param data     待处理数据
    * @param password 密钥
    * @param mode     加解密mode
    * @return
    */
  private def doAES(data: String, key: String, mode: Int): String = {
    try {
      if (StringUtils.isBlank(data) || StringUtils.isBlank(key)) return null
      //判断是加密还是解密
      val encrypt = mode == Cipher.ENCRYPT_MODE
      var content:Array[Byte] = null
      //true 加密内容 false 解密内容
      if (encrypt) {
        content = data.getBytes(defaultCharset)
      }
      else {
        content = parseHexStr2Byte(data)
      }

      //1.构造密钥生成器，指定为AES算法,不区分大小写
      val kgen = KeyGenerator.getInstance(KEY_AES)
      //2.根据ecnodeRules规则初始化密钥生成器
      //生成一个128位的随机源,根据传入的字节数组
      kgen.init(128, new  SecureRandom(key.getBytes))
      //3.产生原始对称密钥
      val secretKey = kgen.generateKey
      //4.获得原始对称密钥的字节数组
      val enCodeFormat = secretKey.getEncoded
      //5.根据字节数组生成AES密钥
      val keySpec = new SecretKeySpec(enCodeFormat, KEY_AES)
      //6.根据指定算法AES自成密码器
      val cipher = Cipher.getInstance(KEY_AES) // 创建密码器
      //7.初始化密码器，第一个参数为加密(Encrypt_mode)或者解密解密(Decrypt_mode)操作，第二个参数为使用的KEY
      cipher.init(mode, keySpec) // 初始化

      val result = cipher.doFinal(content)
      if (encrypt) { //将二进制转换成16进制
        return parseByte2HexStr(result)
      }
      else return new String(result, defaultCharset)
    } catch {
      case e: Exception =>
        logger.error("AES 密文处理异常", e)
    }
    null
  }

  /**
    * 将二进制转换成16进制
    *
    * @param buf
    * @return
    */
  def parseByte2HexStr(buf: Array[Byte]): String = {
    val sb = new StringBuilder
    var i = 0
    while ( {
      i < buf.length
    }) {
      var hex = Integer.toHexString(buf(i) & 0xFF)
      if (hex.length == 1) hex = '0' + hex
      sb.append(hex.toUpperCase)

      {
        i += 1;
        i - 1
      }
    }
    sb.toString
  }

  /**
    * 将16进制转换为二进制
    *
    * @param hexStr
    * @return
    */
  def parseHexStr2Byte(hexStr: String): Array[Byte] = {
    if (hexStr.length < 1) return null
    val result = new Array[Byte](hexStr.length / 2)
    var i = 0
    while ( {
      i < hexStr.length / 2
    }) {
      val high = Integer.parseInt(hexStr.substring(i * 2, i * 2 + 1), 16)
      val low = Integer.parseInt(hexStr.substring(i * 2 + 1, i * 2 + 2), 16)
      result(i) = (high * 16 + low).toByte

      {
        i += 1;
        i - 1
      }
    }
    result
  }

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val content = "0026C5DC5555"
    System.out.println("加密前：" + content)
    System.out.println("加密密钥和解密密钥：" + KEY)
    val encryptString= encrypt(content, KEY)
    System.out.println("加密后：" + encryptString)
    val decryptString = decrypt(encryptString, KEY)
    System.out.println("解密后：" + decryptString)
  }


}
