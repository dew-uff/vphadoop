<results> {
    for $it in doc('expdb/auction.xml')/site/regions/samerica/item
    for $pe in doc('expdb/auction.xml')/site/people/person
    where $pe/profile/@income > 90000 and $pe/profile/interest/@category = $it/incategory/@category
    return
    <match>
        <person>{$pe/name}</person>
        <item>{$it/name}</item>
    </match>
} </results>
