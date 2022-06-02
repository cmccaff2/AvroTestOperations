
#ifndef CPX_HH_1278398428__H_
#define CPX_HH_1278398428__H_


#include "avro/Specific.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"

namespace c {
struct cpx {
    int re;
    int im;
};

}
namespace avro {
template<> struct codec_traits<c::cpx> {
    static void encode(Encoder& e, const c::cpx& v) {
        avro::encode(e, v.re);
        avro::encode(e, v.im);
    }
    static void decode(Decoder& d, c::cpx& v) {
        avro::decode(d, v.re);
        avro::decode(d, v.im);
    }
};

}
#endif
