// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "@openzeppelin/contracts/token/ERC721/ERC721.sol";
import "@openzeppelin/contracts/access/AccessControl.sol";
import "@openzeppelin/contracts/utils/Strings.sol";

contract JSTVIP is ERC721, AccessControl {
    using Strings for uint256;

    bytes32 public constant MINTER_ROLE = keccak256("MINTER_ROLE");

    uint256 private _nextTokenId = 1;
    mapping(address => bool) public hasMinted;
    string private _baseTokenURI;

    event BaseURIUpdated(string newBaseURI);

    constructor(
        string memory name_,
        string memory symbol_,
        string memory baseURI_,
        address admin_,
        address minter_
    ) ERC721(name_, symbol_) {
        require(admin_ != address(0), "admin is zero");
        require(minter_ != address(0), "minter is zero");

        _baseTokenURI = baseURI_;

        _grantRole(DEFAULT_ADMIN_ROLE, admin_);
        _grantRole(MINTER_ROLE, minter_);
    }

    function mint(address to) external onlyRole(MINTER_ROLE) returns (uint256 tokenId) {
        require(to != address(0), "to is zero");
        require(!hasMinted[to], "already minted");

        tokenId = _nextTokenId;
        _nextTokenId += 1;

        hasMinted[to] = true;
        _safeMint(to, tokenId);
    }

    function setBaseURI(string calldata newBaseURI) external onlyRole(DEFAULT_ADMIN_ROLE) {
        _baseTokenURI = newBaseURI;
        emit BaseURIUpdated(newBaseURI);
    }

    function setMinter(address newMinter) external onlyRole(DEFAULT_ADMIN_ROLE) {
        require(newMinter != address(0), "minter is zero");
        _grantRole(MINTER_ROLE, newMinter);
    }

    function revokeMinter(address oldMinter) external onlyRole(DEFAULT_ADMIN_ROLE) {
        _revokeRole(MINTER_ROLE, oldMinter);
    }

    function nextTokenId() external view returns (uint256) {
        return _nextTokenId;
    }

    function tokenURI(uint256 tokenId) public view override returns (string memory) {
        _requireOwned(tokenId);
        return string.concat(_baseURI(), tokenId.toString(), ".json");
    }

    function _baseURI() internal view override returns (string memory) {
        return _baseTokenURI;
    }

    // ✅ REQUIRED because ERC721 and AccessControl both implement supportsInterface
    function supportsInterface(bytes4 interfaceId)
        public
        view
        override(ERC721, AccessControl)
        returns (bool)
    {
        return super.supportsInterface(interfaceId);
    }
}
