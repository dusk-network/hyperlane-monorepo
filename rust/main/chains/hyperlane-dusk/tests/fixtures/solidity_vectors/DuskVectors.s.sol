// SPDX-License-Identifier: MIT OR Apache-2.0
pragma solidity ^0.8.13;

// Writes ../solidity_vectors.jsonl: what Hyperlane's Solidity libraries compute
// for pseudo-random inputs, which tests/differential.rs checks the Dusk shared
// types against. One JSON object per line, every byte string as 0x-hex.
//
// Regenerate from this directory, with the monorepo's Solidity dependencies
// installed (`forge soldeer install` in solidity/):
//
//     forge script DuskVectors.s.sol --tc DuskVectors --offline
//
// The inputs come from a keccak256 chain over a fixed seed, so the output is
// the same on every run.

import {Message} from "@hyperlane/libs/Message.sol";
import {CheckpointLib} from "@hyperlane/libs/CheckpointLib.sol";
import {MerkleLib} from "@hyperlane/libs/Merkle.sol";
import {TokenMessage} from "@hyperlane/token/libs/TokenMessage.sol";
import {MessageIdMultisigIsmMetadata} from "@hyperlane/isms/libs/MessageIdMultisigIsmMetadata.sol";
import {ECDSA} from "@openzeppelin/contracts/utils/cryptography/ECDSA.sol";

interface Vm {
    function writeLine(string calldata path, string calldata data) external;
    function writeFile(string calldata path, string calldata data) external;
    function toString(bytes calldata value) external pure returns (string memory);
    function toString(bytes32 value) external pure returns (string memory);
    function toString(uint256 value) external pure returns (string memory);
}

/// Calldata views of the libraries, which take calldata. Scripts may not call
/// themselves, so the script deploys this and calls it.
contract LibraryViews {
    function formatMessage(
        uint8 version,
        uint32 nonce,
        uint32 origin,
        bytes32 sender,
        uint32 destination,
        bytes32 recipient,
        bytes calldata body
    ) external pure returns (bytes memory) {
        return Message.formatMessage(version, nonce, origin, sender, destination, recipient, body);
    }

    function messageFields(bytes calldata message)
        external
        pure
        returns (uint32 nonce, uint32 origin, bytes32 sender, uint32 destination, bytes32 recipient, bytes memory body)
    {
        return (
            Message.nonce(message),
            Message.origin(message),
            Message.sender(message),
            Message.destination(message),
            Message.recipient(message),
            Message.body(message)
        );
    }

    function tokenFields(bytes calldata body)
        external
        pure
        returns (bytes32 recipient, uint256 amount, bytes memory metadata)
    {
        return (TokenMessage.recipient(body), TokenMessage.amount(body), TokenMessage.metadata(body));
    }

    function metadataFields(bytes calldata metadata)
        external
        pure
        returns (bytes32 hook, bytes32 root, uint32 index)
    {
        return (
            MessageIdMultisigIsmMetadata.originMerkleTreeHook(metadata),
            MessageIdMultisigIsmMetadata.root(metadata),
            MessageIdMultisigIsmMetadata.index(metadata)
        );
    }

    function signatureAt(bytes calldata metadata, uint256 i) external pure returns (bytes memory) {
        return MessageIdMultisigIsmMetadata.signatureAt(metadata, i);
    }

    function signatureCount(bytes calldata metadata) external pure returns (uint256) {
        return MessageIdMultisigIsmMetadata.signatureCount(metadata);
    }

}

contract DuskVectors {
    using MerkleLib for MerkleLib.Tree;

    Vm internal constant vm = Vm(address(uint160(uint256(keccak256("hevm cheat code")))));
    string internal constant OUT = "../solidity_vectors.jsonl";

    bytes32 internal seed = keccak256("hyperlane-dusk differential vectors v1");
    MerkleLib.Tree internal tree;
    LibraryViews internal views;

    function next() internal returns (bytes32) {
        seed = keccak256(abi.encode(seed));
        return seed;
    }

    function nextBytes(uint256 length) internal returns (bytes memory out) {
        out = new bytes(length);
        for (uint256 i = 0; i < length; i += 32) {
            bytes32 word = next();
            for (uint256 j = 0; j < 32 && i + j < length; j++) {
                out[i + j] = word[j];
            }
        }
    }

    function hex32(bytes32 value) internal pure returns (string memory) {
        return vm.toString(value);
    }

    function hexBytes(bytes memory value) internal pure returns (string memory) {
        return vm.toString(value);
    }

    function dec(uint256 value) internal pure returns (string memory) {
        return vm.toString(value);
    }

    function run() external {
        vm.writeFile(OUT, "");
        views = new LibraryViews();
        messages();
        tokenMessages();
        checkpoints();
        announcements();
        merkle();
        metadata();
    }

    function messages() internal {
        uint256[10] memory lengths = [uint256(0), 1, 31, 32, 33, 64, 77, 136, 137, 300];
        for (uint256 n = 0; n < 48; n++) {
            uint256 length = n < lengths.length ? lengths[n] : uint256(next()) % 200;
            uint8 version = n % 7 == 6 ? uint8(uint256(next())) : 3;
            uint32 nonce = n == 0 ? 0 : n == 1 ? type(uint32).max : uint32(uint256(next()));
            uint32 origin = uint32(uint256(next()));
            uint32 destination = n == 2 ? 1146442572 : uint32(uint256(next()));
            bytes32 sender = next();
            bytes32 recipient = n % 3 == 0 ? bytes32(uint256(uint160(uint256(next())))) : next();
            bytes memory body = nextBytes(length);
            bytes memory encoded = views.formatMessage(version, nonce, origin, sender, destination, recipient, body);
            (uint32 rNonce, uint32 rOrigin, bytes32 rSender, uint32 rDestination, bytes32 rRecipient, bytes memory rBody) =
                views.messageFields(encoded);
            require(
                rNonce == nonce && rOrigin == origin && rSender == sender && rDestination == destination
                    && rRecipient == recipient && keccak256(rBody) == keccak256(body),
                "message fields"
            );
            string memory line = string.concat(
                '{"kind":"message","version":', dec(version), ',"nonce":', dec(nonce), ',"origin":', dec(origin)
            );
            line = string.concat(
                line, ',"sender":"', hex32(sender), '","destination":', dec(destination), ',"recipient":"', hex32(recipient)
            );
            line = string.concat(
                line, '","body":"', hexBytes(body), '","encoded":"', hexBytes(encoded), '","id":"', hex32(Message.id(encoded)), '"}'
            );
            vm.writeLine(OUT, line);
        }
    }

    function tokenMessages() internal {
        uint256[10] memory amounts = [
            uint256(0),
            1,
            type(uint64).max,
            uint256(type(uint64).max) + 1,
            type(uint128).max,
            uint256(type(uint128).max) + 1,
            1 << 255,
            type(uint256).max,
            1e18,
            1e9
        ];
        for (uint256 n = 0; n < 40; n++) {
            uint256 amount = n < amounts.length ? amounts[n] : n % 2 == 0 ? uint256(uint64(uint256(next()))) : uint256(next());
            bytes32 recipient;
            uint256 kind = n % 5;
            if (kind == 0) {
                recipient = bytes32(0);
            } else if (kind == 1) {
                // An EVM address, left-padded as TypeCasts.addressToBytes32 does.
                recipient = bytes32(uint256(uint160(uint256(next()))));
            } else if (kind == 2) {
                // The same, padded on the wrong side (security.md, LOW-5).
                recipient = bytes32(uint256(uint160(uint256(next()))) << 96);
            } else if (kind == 3) {
                recipient = bytes32(type(uint256).max);
            } else {
                // A Dusk contract ID or the keccak256 of a BLS key.
                recipient = next();
            }
            bytes memory metadataBytes = nextBytes(n % 4 == 0 ? 0 : uint256(next()) % 40);
            bytes memory encoded = TokenMessage.format(recipient, amount, metadataBytes);
            (bytes32 rRecipient, uint256 rAmount, bytes memory rMetadata) = views.tokenFields(encoded);
            require(
                rRecipient == recipient && rAmount == amount && keccak256(rMetadata) == keccak256(metadataBytes),
                "token fields"
            );
            string memory line = string.concat(
                '{"kind":"token_message","recipient":"', hex32(recipient), '","amount":"', hex32(bytes32(amount))
            );
            line = string.concat(line, '","metadata":"', hexBytes(metadataBytes), '","encoded":"', hexBytes(encoded), '"}');
            vm.writeLine(OUT, line);
        }
    }

    function checkpoints() internal {
        for (uint256 n = 0; n < 48; n++) {
            uint32 origin = n == 0 ? 0 : n == 1 ? type(uint32).max : n == 2 ? 1146442572 : uint32(uint256(next()));
            bytes32 hook = next();
            bytes32 root = n == 3 ? bytes32(0) : next();
            uint32 index = n == 4 ? 0 : n == 5 ? type(uint32).max : uint32(uint256(next()));
            bytes32 messageId = next();
            string memory line = string.concat(
                '{"kind":"checkpoint","origin":', dec(origin), ',"merkle_tree_hook":"', hex32(hook), '","root":"', hex32(root)
            );
            line = string.concat(
                line,
                '","index":',
                dec(index),
                ',"message_id":"',
                hex32(messageId),
                '","domain_hash":"',
                hex32(CheckpointLib.domainHash(origin, hook))
            );
            line = string.concat(line, '","digest":"', hex32(CheckpointLib.digest(origin, hook, root, index, messageId)), '"}');
            vm.writeLine(OUT, line);
        }
    }

    /// ValidatorAnnounce.getAnnouncementDigest, with the mailbox as the
    /// bytes32 that `_domainHash` packs (an EVM address left-padded, or a Dusk
    /// contract ID).
    function announcementDigest(uint32 domain, bytes32 mailbox, bytes memory location) internal pure returns (bytes32) {
        bytes32 domainHash = keccak256(abi.encodePacked(domain, mailbox, "HYPERLANE_ANNOUNCEMENT"));
        return ECDSA.toEthSignedMessageHash(keccak256(abi.encodePacked(domainHash, location)));
    }

    function announcements() internal {
        bytes[6] memory fixedLocations = [
            bytes(""),
            bytes("s3://hyperlane-testnet/us-east-1/validator-0"),
            bytes("file:///var/lib/hyperlane/checkpoints"),
            bytes("gs://b\xc3\xbccket/\xe2\x9c\x93"),
            bytes("s3://a"),
            bytes("x")
        ];
        for (uint256 n = 0; n < 24; n++) {
            uint32 domain = n == 0 ? 1146442572 : uint32(uint256(next()));
            bytes32 mailbox = n % 2 == 0 ? next() : bytes32(uint256(uint160(uint256(next()))));
            // ASCII locations of any length beyond the fixed ones.
            bytes memory location;
            if (n < fixedLocations.length) {
                location = fixedLocations[n];
            } else {
                location = nextBytes(uint256(next()) % 120);
                for (uint256 i = 0; i < location.length; i++) {
                    location[i] = bytes1(uint8(0x21 + uint8(location[i]) % 0x5e));
                }
            }
            string memory line = string.concat(
                '{"kind":"announcement","domain":', dec(domain), ',"mailbox":"', hex32(mailbox), '","location":"', hexBytes(location)
            );
            line = string.concat(line, '","digest":"', hex32(announcementDigest(domain, mailbox, location)), '"}');
            vm.writeLine(OUT, line);
        }
    }

    function merkle() internal {
        bytes32[32] memory zeroes = MerkleLib.zeroHashes();
        for (uint256 level = 0; level < 32; level++) {
            vm.writeLine(
                OUT, string.concat('{"kind":"zero_hash","level":', dec(level), ',"hash":"', hex32(zeroes[level]), '"}')
            );
        }
        // Leaf i is keccak256(abi.encode(i)). Roots at every count to 66 and on
        // each side of 2^7 ... 2^12.
        vm.writeLine(OUT, string.concat('{"kind":"merkle_root","count":0,"root":"', hex32(tree.root()), '"}'));
        for (uint256 i = 0; i < 4097; i++) {
            tree.insert(keccak256(abi.encode(i)));
            uint256 count = i + 1;
            bool boundary = false;
            for (uint256 k = 7; k <= 12; k++) {
                uint256 power = 1 << k;
                if (count + 1 == power || count == power || count == power + 1) {
                    boundary = true;
                }
            }
            if (count <= 66 || boundary) {
                vm.writeLine(
                    OUT, string.concat('{"kind":"merkle_root","count":', dec(count), ',"root":"', hex32(tree.root()), '"}')
                );
            }
        }
    }

    function metadata() internal {
        for (uint256 n = 0; n < 24; n++) {
            bytes32 hook = next();
            bytes32 root = next();
            uint32 index = n == 0 ? type(uint32).max : uint32(uint256(next()));
            uint256 count = n % 6;
            bytes memory encoded = abi.encodePacked(hook, root, index);
            for (uint256 i = 0; i < count; i++) {
                encoded = bytes.concat(encoded, nextBytes(65));
            }
            // Every fourth: trailing bytes that aren't a whole signature.
            uint256 trailing = n % 4 == 3 ? 1 + uint256(next()) % 64 : 0;
            encoded = bytes.concat(encoded, nextBytes(trailing));
            (bytes32 rHook, bytes32 rRoot, uint32 rIndex) = views.metadataFields(encoded);
            require(rHook == hook && rRoot == root && rIndex == index, "metadata fields");
            string memory line = string.concat(
                '{"kind":"multisig_metadata","metadata":"', hexBytes(encoded), '","merkle_tree_hook":"', hex32(hook)
            );
            line = string.concat(line, '","root":"', hex32(root), '","index":', dec(index), ',"signatures":[');
            for (uint256 i = 0; i < count; i++) {
                line = string.concat(line, i == 0 ? '"' : ',"', hexBytes(views.signatureAt(encoded, i)), '"');
            }
            // signatureCount reverts unless the signatures are whole.
            string memory counted = "null";
            try views.signatureCount(encoded) returns (uint256 counted_) {
                counted = dec(counted_);
            } catch {}
            line = string.concat(line, '],"signature_count":', counted, ',"trailing":', dec(trailing), "}");
            vm.writeLine(OUT, line);
        }
    }
}
