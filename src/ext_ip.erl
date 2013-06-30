-module(ext_ip).
-export([detect_external_ip/0]).

detect_external_ip() ->
    random:seed(now()),
    detect_external_ip(10, 3, detector_urls(), user_agent(), []).


detect_external_ip(0, _Sources, [], _Agents, _Acc) ->
    {error, urls_runned_out};
detect_external_ip(0, _Sources, _URLs, _Agents, _Acc) ->
    {error, attempts_runned_out};
detect_external_ip(_, 0, _URLs, _Agents, Acc) ->
    {ok, most_popular_element_of_the_list(Acc)};
detect_external_ip(Attempts, Sources, URLs, Agents, Acc) 
    when Attempts > 0, Sources > 0 ->
    URL   = random_element(URLs),
    Agent = random_element(Agents),
    case lhttpc:request(URL, get, [{<<"User-Agent">>, Agent}], 5000) of
        {ok, {{200, "OK"}, _Headers, Body}} ->
            try
            BodyS = binary_to_list(Body),
            [A,B,C,D] = string:tokens(BodyS, "."),
            IP = list_to_tuple([to_integer(X) || X <- [A,B,C,D]]),
            detect_external_ip(Attempts, Sources-1,
                               lists:delete(URL, URLs), Agents, [IP|Acc])
            catch error:_Reason ->
                detect_external_ip(Attempts-1, Sources,
                                   lists:delete(URL, URLs), Agents, Acc)
            end
    end.

most_popular_element_of_the_list([_|_]=List) ->
    [H|T] = lists:sort(List),
    most_popular_element_of_the_list_1(T, H, 1, H, 1).

most_popular_element_of_the_list_1([H|T], H, N, BestH, BestN) ->
    most_popular_element_of_the_list_1(T, H, N+1, BestH, BestN);
most_popular_element_of_the_list_1([X|T], H, N, _, BestN)
    when N > BestN ->
    %% Better result.
    most_popular_element_of_the_list_1(T, X, 0, H, N);
most_popular_element_of_the_list_1([X|T], _, _, BestH, BestN) ->
    most_popular_element_of_the_list_1(T, X, 0, BestH, BestN);
most_popular_element_of_the_list_1([], H, N, _, BestN)
    when N > BestN ->
    %% Better result.
    H;
most_popular_element_of_the_list_1([], _, _, BestH, _) ->
    BestH.
            
to_integer(S) ->
    case string:to_integer(S) of
        {error, Reason} -> error(Reason);
        {I, _} -> I
    end.

random_element(List) ->
    lists:nth(random:uniform(length(List)), List).

detector_urls() ->
    ["http://checkip.amazonaws.com/"
    ,"http://api.exip.org/?call=ip"
    ,"http://ifconfig.me/"
    ,"http://icanhazip.com/"
    ,"http://myexternalip.com/raw"
    ,"http://curlmyip.com/"
    ,"http://queryip.net/ip/"
    ,"http://whatismyip.akamai.com/"
    ,"http://tnx.nl/ip"
    ,"http://myip.dnsomatic.com/"
    ,"http://ip.appspot.com/"
    ,"http://ident.me/"
    ].


user_agent() ->
    ["curl/7.20.0 (i686-pc-linux-gnu) libcurl/7.20.0 OpenSSL/0.9.8n zlib/1.2.4"
    ,"curl/7.20.0 (i386-apple-darwin9.8.0) libcurl/7.20.0 OpenSSL/0.9.8m zlib/1.2.3 libidn/1.16"
    ,"curl/7.19.7 (universal-apple-darwin10.0) libcurl/7.19.7 OpenSSL/0.9.8l zlib/1.2.3"
    ,"curl/7.19.7 (i486-pc-linux-gnu) libcurl/7.19.7 OpenSSL/0.9.8k zlib/1.2.3.3 libidn/1.15"
    ,"curl/7.19.7 (i386-redhat-linux-gnu) libcurl/7.19.7 NSS/3.12.5.0 zlib/1.2.3 libidn/1.9 libssh2/1.2.2"
    ,"curl/7.19.7 (i386-apple-darwin9.8.0) libcurl/7.19.7 zlib/1.2.3"
    ,"curl/7.19.6 (i686-pc-cygwin) libcurl/7.19.6 OpenSSL/0.9.8n zlib/1.2.3 libidn/1.18 libssh2/1.2"
    ,"curl/7.19.6 (i386-redhat-linux-gnu) libcurl/7.19.6 NSS/3.12.4.5 zlib/1.2.3 libidn/1.9 libssh2/1.2"
    ,"curl/7.19.6 (i386-pc-win32) libcurl/7.19.6 OpenSSL/0.9.8k zlib/1.2.3"
    ,"curl/7.19.5 (i586-pc-mingw32msvc) libcurl/7.19.5 zlib/1.2.3"
    ,"curl/7.19.5 (i486-pc-linux-gnu) libcurl/7.19.5 OpenSSL/0.9.8g zlib/1.2.3.3 libidn/1.15"
    ,"curl/7.19.4 (universal-apple-darwin10.0) libcurl/7.19.4 OpenSSL/0.9.8k zlib/1.2.3"
    ,"curl/7.19.4 (i686-pc-cygwin) libcurl/7.19.4 OpenSSL/0.9.8k zlib/1.2.3 libidn/1.9 libssh2/1.0"
    ,"curl/7.19.2 (i386-pc-win32) libcurl/7.19.2 OpenSSL/0.9.8i zlib/1.2.3 libidn/1.11 libssh2/0.18"
    ,"curl/7.19.2 (i386-pc-win32) libcurl/7.19.2 OpenSSL/0.9.8c zlib/1.2.3"
    ,"curl/7.19.0 (x86_64-suse-linux-gnu) libcurl/7.19.0 OpenSSL/0.9.8h zlib/1.2.3 libidn/1.10"
    ,"curl/7.18.2 (x86_64-pc-linux-gnu) libcurl/7.18.2 OpenSSL/0.9.8g zlib/1.2.3.3 libidn/1.8 libssh2/0.18"
    ,"curl/7.18.1 (i686-suse-linux-gnu) libcurl/7.18.1 OpenSSL/0.9.8g zlib/1.2.3 libidn/1.8"
    ,"curl/7.18.0 (x86_64-pc-linux-gnu) libcurl/7.18.0 OpenSSL/0.9.8g zlib/1.2.3.3 libidn/1.1"
    ,"curl/7.17.1 (x86_64-pc-linux-gnu) libcurl/7.17.1 OpenSSL/0.9.8g zlib/1.2.3"
    ,"curl/7.16.4 (i486-pc-linux-gnu) libcurl/7.16.4 OpenSSL/0.9.8e zlib/1.2.3.3 libidn/1.0"
    ,"curl/7.16.3 (powerpc-apple-darwin8.0) libcurl/7.16.3 OpenSSL/0.9.7l zlib/1.2.3"
    ,"curl/7.16.2 (x86_64-redhat-linux-gnu) libcurl/7.16.2 OpenSSL/0.9.8b zlib/1.2.3 libidn/0.6.8"
    ,"curl/7.16.1 (i386-pc-win32) libcurl/7.16.1 OpenSSL/0.9.8h zlib/1.2.3"
    ,"curl/7.15.5 (x86_64-redhat-linux-gnu) libcurl/7.15.5 OpenSSL/0.9.8b zlib/1.2.3 libidn/0.6.5"
    ,"curl/7.15.4 (i686-pc-linux-gnu) libcurl/7.15.4 OpenSSL/0.9.7e zlib/1.2.3"
    ,"curl/7.15.3 (sparc64--netbsd) libcurl/7.15.3 OpenSSL/0.9.7d zlib/1.1.4 libidn/0.6.3"
    ,"curl/7.15.1 (x86_64-suse-linux) libcurl/7.15.1 OpenSSL/0.9.8a zlib/1.2.3 libidn/0.6.0"
    ,"curl/7.15.1 (i486-pc-linux-gnu) libcurl/7.15.1 OpenSSL/0.9.8a zlib/1.2.3 libidn/0.5.18"
    ,"curl/7.15.0 (i386-portbld-freebsd5.4) libcurl/7.15.0 OpenSSL/0.9.7e zlib/1.2.1"
    ,"curl/7.14.0 (i386-portbld-freebsd5.4) libcurl/7.14.0 OpenSSL/0.9.7e zlib/1.2.1"
    ,"curl/7.13.2 (i386-pc-linux-gnu) libcurl/7.13.2 OpenSSL/0.9.7e zlib/1.2.2 libidn/0.5.13"
    ,"curl/7.13.1 (powerpc-apple-darwin8.0) libcurl/7.13.1 OpenSSL/0.9.7l zlib/1.2.3"
    ,"curl/7.12.1 (i686-redhat-linux-gnu) libcurl/7.12.1 OpenSSL/0.9.7a zlib/1.2.1.2 libidn/0.5.6"
    ,"curl/7.11.1 (i686-redhat-linux-gnu) libcurl/7.11.1 OpenSSL/0.9.7a ipv6 zlib/1.2.1.2"
    ,"curl/7.11.1 (i386-redhat-linux-gnu) libcurl/7.11.1 OpenSSL/0.9.7a ipv6 zlib/1.2.1.2"
    ,"curl/7.10.6 (i386-redhat-linux-gnu) libcurl/7.10.6 OpenSSL/0.9.7a ipv6 zlib/1.1.4"
    ].

