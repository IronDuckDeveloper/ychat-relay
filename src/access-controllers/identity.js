import { peerIdFromString } from '@libp2p/peer-id';

const type = 'helia';

const verifyIdentity = async (identity) => {
  try {
    const peerId = peerIdFromString(identity.id);
    if (!peerId.publicKey) return false;

    const dataString = identity.publicKey + identity.signatures.id;
    const dataBytes = new TextEncoder().encode(dataString);
    const sigBytes = Uint8Array.from(Buffer.from(identity.signatures.publicKey, 'hex'));

    const result = await peerId.publicKey.verify(dataBytes, sigBytes);
    return result;
  } catch (err) {
    console.error('❌ [HeliaIdentityProvider] Ошибка верификации:', err.message);
    return false;
  }
};

// Релей никогда сам не создаёт identity этого типа — эти два метода
// нужны только чтобы удовлетворить форму интерфейса, вызываться не будут.
const HeliaIdentityProvider = () => async () => {
  const getId = async () => { throw new Error('Relay does not create helia-type identities'); };
  const signIdentity = async () => { throw new Error('Relay does not create helia-type identities'); };
  return { type, getId, signIdentity };
};

HeliaIdentityProvider.verifyIdentity = verifyIdentity;
HeliaIdentityProvider.type = type;

export default HeliaIdentityProvider;