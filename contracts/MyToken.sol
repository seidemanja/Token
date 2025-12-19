// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
import "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/utils/Pausable.sol";

/**
 * @title MyToken
 * @dev ERC20 token 
 */

contract MyToken is ERC20, Ownable, Pausable {
constructor(
string memory name_,
string memory symbol_,
uint256 initialSupply
    ) ERC20(name_, symbol_) Ownable(msg.sender) {
_mint(msg.sender, initialSupply);
    }

// Pausable functionality
function pause() public onlyOwner {
_pause();  // OpenZeppelin emits Paused automatically
    }

function unpause() public onlyOwner {
_unpause();  // OpenZeppelin emits Unpaused automatically
    }

// Override ERC20 _update function to support Pausable
function _update(
address from,
address to,
uint256 amount
    ) internal override whenNotPaused {
super._update(from, to, amount);
    }

}