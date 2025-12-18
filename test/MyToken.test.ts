import { expect } from "chai";
import { ethers } from "hardhat";
import { MyToken } from "../typechain-types";
import { SignerWithAddress } from "@nomicfoundation/hardhat-ethers/signers";

describe("MyToken", function () {
  let myToken: MyToken;
  let owner: SignerWithAddress;
  let addr1: SignerWithAddress;
  let addr2: SignerWithAddress;

  // This runs before each test
  beforeEach(async function () {
    // Get signers (test accounts)
    [owner, addr1, addr2] = await ethers.getSigners();

    // Deploy the contract
    const MyTokenFactory = await ethers.getContractFactory("MyToken");
    const initialSupply = ethers.parseUnits("1000000", 18);
    myToken = await MyTokenFactory.deploy(initialSupply);
    await myToken.waitForDeployment();
  });

  describe("Deployment", function () {
    it("Should set the right token name", async function () {
      expect(await myToken.name()).to.equal("MyToken");
    });

    it("Should set the right token symbol", async function () {
      expect(await myToken.symbol()).to.equal("MTK");
    });

    it("Should assign the total supply to the owner", async function () {
      const ownerBalance = await myToken.balanceOf(owner.address);
      const totalSupply = await myToken.totalSupply();
      expect(ownerBalance).to.equal(totalSupply);
    });

    it("Should have correct initial supply", async function () {
      const expectedSupply = ethers.parseUnits("1000000", 18);
      expect(await myToken.totalSupply()).to.equal(expectedSupply);
    });
  });

  describe("Transactions", function () {
    it("Should transfer tokens between accounts", async function () {
      // Transfer 50 tokens from owner to addr1
      const transferAmount = ethers.parseUnits("50", 18);
      await myToken.transfer(addr1.address, transferAmount);
      
      const addr1Balance = await myToken.balanceOf(addr1.address);
      expect(addr1Balance).to.equal(transferAmount);
    });

    it("Should fail if sender doesn't have enough tokens", async function () {
      const initialOwnerBalance = await myToken.balanceOf(owner.address);
      
      // Try to send more tokens than owner has
      const tooMuch = initialOwnerBalance + ethers.parseUnits("1", 18);
      
      await expect(
        myToken.connect(addr1).transfer(owner.address, tooMuch)
      ).to.be.reverted;
    });

    it("Should update balances after transfers", async function () {
      const initialOwnerBalance = await myToken.balanceOf(owner.address);
      const transferAmount = ethers.parseUnits("100", 18);

      // Transfer from owner to addr1
      await myToken.transfer(addr1.address, transferAmount);

      // Transfer from owner to addr2
      await myToken.transfer(addr2.address, transferAmount);

      const finalOwnerBalance = await myToken.balanceOf(owner.address);
      expect(finalOwnerBalance).to.equal(
        initialOwnerBalance - (transferAmount * 2n)
      );

      const addr1Balance = await myToken.balanceOf(addr1.address);
      expect(addr1Balance).to.equal(transferAmount);

      const addr2Balance = await myToken.balanceOf(addr2.address);
      expect(addr2Balance).to.equal(transferAmount);
    });

    it("Should emit Transfer event on transfer", async function () {
      const transferAmount = ethers.parseUnits("50", 18);
      
      await expect(myToken.transfer(addr1.address, transferAmount))
        .to.emit(myToken, "Transfer")
        .withArgs(owner.address, addr1.address, transferAmount);
    });
  });

  describe("Allowances", function () {
    it("Should approve tokens for delegated transfer", async function () {
      const approveAmount = ethers.parseUnits("100", 18);
      
      await myToken.approve(addr1.address, approveAmount);
      
      const allowance = await myToken.allowance(owner.address, addr1.address);
      expect(allowance).to.equal(approveAmount);
    });

    it("Should allow transferFrom with sufficient allowance", async function () {
      const approveAmount = ethers.parseUnits("100", 18);
      const transferAmount = ethers.parseUnits("50", 18);

      // Owner approves addr1 to spend tokens
      await myToken.approve(addr1.address, approveAmount);

      // addr1 transfers from owner to addr2
      await myToken.connect(addr1).transferFrom(
        owner.address,
        addr2.address,
        transferAmount
      );

      const addr2Balance = await myToken.balanceOf(addr2.address);
      expect(addr2Balance).to.equal(transferAmount);
    });

    it("Should fail transferFrom without sufficient allowance", async function () {
      const transferAmount = ethers.parseUnits("50", 18);

      await expect(
        myToken.connect(addr1).transferFrom(owner.address, addr2.address, transferAmount)
      ).to.be.reverted;
    });
  });
});