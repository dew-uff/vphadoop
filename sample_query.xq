<results> {   for $it in doc('auction')/site/regions/africa/item   for $co in doc('auction')/site/closed_auctions/closed_auction   where $co/itemref/@item = $it/@id   and $it/payment = "Cash"    return     <itens>      {$co/price}      {$co/date}      {$co/quantity}      {$co/type}      {$it/payment}      {$it/location}      {$it/from}      {$it/to}    </itens> }</results>
