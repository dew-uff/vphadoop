<results> {
let $p := collection('auction')//closed_auction
return
    <summary>
        <total_items>{count($p)}</total_items>
        <avg_price>{avg($p/price)}</avg_price>
    </summary>
} </results>
