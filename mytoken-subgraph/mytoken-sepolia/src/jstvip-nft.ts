import { BigInt, Bytes } from "@graphprotocol/graph-ts";
import { Transfer as NftTransferEvent } from "../generated/JSTVIP/ERC721";
import { NftTransfer, NftToken, NftOwner } from "../generated/schema";

const ZERO = Bytes.fromHexString("0x0000000000000000000000000000000000000000") as Bytes;

function tokenEntityId(contract: Bytes, tokenId: BigInt): string {
  return contract.toHexString().concat("-").concat(tokenId.toString());
}

function ownerEntityId(owner: Bytes): string {
  return owner.toHexString();
}

function getOrCreateOwner(contract: Bytes, owner: Bytes): NftOwner {
  const id = ownerEntityId(owner);
  let ent = NftOwner.load(id);
  if (ent == null) {
    ent = new NftOwner(id);
    ent.contract = contract;
    ent.balance = BigInt.fromI32(0);
  }
  return ent as NftOwner;
}

export function handleNftTransfer(event: NftTransferEvent): void {
  const contract = event.address;
  const from = event.params.from;
  const to = event.params.to;
  const tokenId = event.params.tokenId;

  // Immutable transfer log entity
  const transferId = event.transaction.hash
    .toHexString()
    .concat("-")
    .concat(event.logIndex.toString());

  const t = new NftTransfer(transferId);
  t.contract = contract;
  t.from = from;
  t.to = to;
  t.tokenId = tokenId;
  t.blockNumber = event.block.number;
  t.blockTimestamp = event.block.timestamp;
  t.transactionHash = event.transaction.hash;
  t.logIndex = event.logIndex;
  t.isMint = from.equals(ZERO);
  t.isBurn = to.equals(ZERO);
  t.save();

  // Token current state
  const tokId = tokenEntityId(contract, tokenId);
  let tok = NftToken.load(tokId);

  if (tok == null) {
    tok = new NftToken(tokId);
    tok.contract = contract;
    tok.tokenId = tokenId;

    tok.mintedAtBlock = event.block.number;
    tok.mintedAtTimestamp = event.block.timestamp;
    tok.mintTxHash = event.transaction.hash;
  }

  if (to.equals(ZERO)) {
    tok.burnedAtBlock = event.block.number;
    tok.burnedAtTimestamp = event.block.timestamp;
    tok.burnTxHash = event.transaction.hash;
    tok.owner = ZERO;
  } else {
    tok.owner = to;
  }
  tok.save();

  // Owner balances (derived)
  if (!from.equals(ZERO)) {
    let fromOwner = getOrCreateOwner(contract, from);
    if (fromOwner.balance.gt(BigInt.fromI32(0))) {
      fromOwner.balance = fromOwner.balance.minus(BigInt.fromI32(1));
    }
    fromOwner.save();
  }

  if (!to.equals(ZERO)) {
    let toOwner = getOrCreateOwner(contract, to);
    toOwner.balance = toOwner.balance.plus(BigInt.fromI32(1));
    toOwner.save();
  }
}
