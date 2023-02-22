import org.scalatest._
class SupermarketCheckoutSpec extends App {

  "SupermarketCheckout" should "return 0 for empty cart" in {
    val checkout = new SupermarketCheckout()
    checkout.total() should be(0)
  }

  it should "return the correct total for cart with items A, B, C, D" in {
    val checkout = new SupermarketCheckout()
    checkout.scan("A")
    checkout.scan("B")
    checkout.scan("C")
    checkout.scan("D")
    checkout.total() should be(115)
  }

  it should "return the correct total for cart with 3 As and 2 Bs" in {
    val checkout = new SupermarketCheckout()
    checkout.scan("A")
    checkout.scan("A")
    checkout.scan("A")
    checkout.scan("B")
    checkout.scan("B")
    checkout.total() should be(175)
  }

  it should "return the correct total for cart with 5 As and 5 Bs" in {
    val checkout = new SupermarketCheckout()
    checkout.scan("A")
    checkout.scan("A")
    checkout.scan("A")
    checkout.scan("A")
    checkout.scan("A")
    checkout.scan("B")
    checkout.scan("B")
    checkout.scan("B")
    checkout.scan("B")
    checkout.scan("B")
    checkout.total() should be(370)
  }
}

class SupermarketCheckout {
  private var cart: Map[String, Int] = Map()

  private val prices: Map[String, Int] = Map(
    "A" -> 50,
    "B" -> 30,
    "C" -> 20,
    "D" -> 15
  )
  private val specialOffers: Map[String, (Int, Int)] = Map(
    "A" -> (3, 130),
    "B" -> (2, 45)
  )

  def scan(item: String): Unit = {
    if (cart.contains(item)) {
      cart = cart + (item -> (cart(item) + 1))
    } else {
      cart = cart + (item -> 1)
    }
  }

  def total(): Int = {
    cart.map(item => {
      val (itemName, itemCount) = item
      val itemPrice = prices(itemName)
      val specialOffer = specialOffers.get(itemName)

      specialOffer match {
        case Some((offerCount, offerPrice)) =>
          val regularPriceCount = itemCount % offerCount
          val offerPriceCount = itemCount / offerCount
          (regularPriceCount * itemPrice) + (offerPriceCount * offerPrice)
        case None =>
          itemCount * itemPrice
      }
    }).sum
  }
