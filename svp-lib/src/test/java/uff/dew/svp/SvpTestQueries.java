package uff.dew.svp;

public class SvpTestQueries {

    public final static String query_sd_regular = "<results> {"
            + "    for $p in doc('auction.xml')/site/people/person"
            + "    let $e := $p/homepage"
            + "    where count($e) = 0"
            + "    return"
            + "        <people_without_homepage>"
            + "            {$p/name}"
            + "        </people_without_homepage>"
            + "} </results>";
    
    public final static String query_sd_order_by = "<results> {"
            + "    for $pe in doc('auction.xml')/site/people/person "
            + "    let $int := $pe/profile/interest "
            + "    where $pe/profile/business = \"Yes\" and count($int) > 1"
                    + "    order by $pe/name"
                    + "    return"
                    + "    <people>"
                    + "        {$pe}"
                    + "    </people>"
                    + "} </results>";

    public final static String query_sd_order_by_descending = "<results> {"
            + "    for $pe in doc('auction.xml')/site/people/person "
            + "    let $int := $pe/profile/interest "
            + "    where $pe/profile/business = \"Yes\" and count($int) > 1"
                    + "    order by $pe/name descending"
                    + "    return"
                    + "    <people>"
                    + "        {$pe}"
                    + "    </people>"
                    + "} </results>";
    
    public final static String query_sd_join = "<results> {   "
            + "for $it in doc('auction.xml')/site/regions/africa/item   "
            + "for $co in doc('auction.xml')/site/closed_auctions/closed_auction   "
            + "where $co/itemref/@item = $it/@id   and $it/payment = \"Cash\"    "
                    + "return     <itens>"
                    + "      {$co/price}"
                    + "      {$co/date}"
                    + "      {$co/quantity}"
                    + "      {$co/type}"
                    + "      {$it/payment}"
                    + "      {$it/location}"
                    + "      {$it/from}"
                    + "      {$it/to}"
                    + "    </itens>"
                    + " }</results>";
    
    public final static String query_sd_aggregation = "<results> {"
            + "    let $p := doc('auction.xml')/site/closed_auctions/closed_auction"
            + "    return"
            + "        <summary>"
            + "            <cont>{count($p)}</cont>"
            + "            <media>{avg($p/price)}</media>"
            + "        </summary>"
            + "} </results>";

    public static final String query_sd_incomplete_path = 
            " <results> \r\n" +
            " { \r\n"+
            "   for $p in document('auction.xml')//person \r\n"+
            " return \r\n"+
            "  <person> \r\n"+
            "    {$p/name} \r\n"+
            "  </person> \r\n"+
            " } \r\n"+ 
            " </results>"; 
    
    public static final String query_sd_incomplete_path_join = 
            "<results> \r\n"
            + "{ \r\n"
            + "  for $p in document('auction.xml')//person \r\n"
            + "  for $a in document('auction.xml')//closed_auction \r\n"
            + "  where $p/@id = $a/@person \r\n"
            + "      return \r\n"
            + "        <buyer> \r\n"
            + "          {$p/name} \r\n"
            + "          {$a/price} \r\n"
            + "        </buyer> \r\n"
            + "    } </results>";
    
    public static final String query_md_regular = 
            "<results> {\n"
            + "for $op in collection('md')/site/open_auctions/open_auction\n"
            + "let $bd := $op/bidder where count($op/bidder) > 5\n"
            + "return\n"
            + "<open_auctions_with_more_than_5_bidders>\n"
            +   "<auction>\n"
            +       "{$op}\n"
            +   "</auction>\n"
            +   "<qty_bidder>\n"
            +   "{count($op/bidder)}\n"
            +   "</qty_bidder>\n"
            + "</open_auctions_with_more_than_5_bidders>\n"
            +"} </results>";
    
    public static final String query_md_incomplete_path = 
            " <results> \r\n" +
            " { \r\n"+
            "   for $p in collection('auctions')//person \r\n"+
            " return \r\n"+
            "  <person> \r\n"+
            "    {$p/name} \r\n"+
            "  </person> \r\n"+
            " } \r\n"+ 
            " </results>"; 
    
    public static final String query_md_incomplete_path_join = 
            "<results> \r\n"
            + "{ \r\n"
            + "  for $p in collection('auctions')//person \r\n"
            + "  for $a in collection('auctions')//closed_auction \r\n"
            + "  where $p/@id = $a/@person \r\n"
            + "      return \r\n"
            + "        <buyer> \r\n"
            + "          {$p/name} \r\n"
            + "          {$a/price} \r\n"
            + "        </buyer> \r\n"
            + "    } </results>";
}
