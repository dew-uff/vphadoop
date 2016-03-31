<results> {
    for $it in collection('auction')/site/regions/africa/item
    for $co in collection('auction')/site/closed_auctions/closed_auction
    where $co/itemref/@item = $it/@id and $it/payment = "Cash"
    return
        <item_cash>
            {$co/price}
            {$co/date}
            {$co/quantity}
            {$co/type}
            {$it/payment}
            {$it/location}
            {$it/from}
            {$it/to}
        </item_cash>
} </results>
