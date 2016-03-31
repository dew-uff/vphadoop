<results> {
    for $it in doc('auction.xml')/site/regions/samerica/item
    for $pe in doc('auction.xml')/site/people/person
    where $pe/profile/interest/@category = $it/incategory/@category
    return
    <people>
        {$pe}
    </people>
} </results>
