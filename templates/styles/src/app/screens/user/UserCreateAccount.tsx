import { useState, useRef } from "react";
import { useNavigate } from "react-router";
import { BackButton } from "../../components/BackButton";
import { AnimatedBackground } from "../../components/AnimatedBackground";

export function UserCreateAccount() {
  const navigate = useNavigate();
  const [formData, setFormData] = useState({
    fullName: "",
    email: "",
    phoneNumber: "",
    organizationName: "",
    purpose: "",
  });
  const [hoveredInput, setHoveredInput] = useState<string | null>(null);
  const [isContainerHovered, setIsContainerHovered] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  return (
    <div className="min-h-screen relative">
      <AnimatedBackground />
      
      <div className="relative z-10">
        <div className="bg-[#1F3A5F] text-white px-10 py-6 shadow-xl relative z-50">
          <div className="max-w-[1800px] mx-auto flex justify-between items-center">
            <h1 
              className="text-4xl m-0 font-black tracking-tighter cursor-pointer hover:opacity-80 transition-opacity" 
              style={{ fontFamily: 'var(--font-head)', color: 'white' }}
              onClick={() => navigate("/")}
            >
              STATXTRACT
            </h1>
          </div>
        </div>

        <div className="max-w-[1000px] mx-auto px-6 py-20">
          <div className="mb-10">
            <BackButton to="/user/module-selection" />
          </div>
          
          <h2 className="text-5xl text-[#1C1C1C] mb-12 text-center" style={{ fontFamily: 'var(--font-head)', fontWeight: 800 }}>
            Create Your Account
          </h2>

          <div 
            ref={containerRef}
            onMouseEnter={() => setIsContainerHovered(true)}
            onMouseLeave={() => setIsContainerHovered(false)}
            className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-2xl p-12 relative overflow-hidden shadow-2xl"
            style={{
              borderColor: isContainerHovered ? '#2E7D32' : '#D1D5DB',
              boxShadow: isContainerHovered 
                ? '0 30px 70px rgba(46, 125, 50, 0.25)'
                : 'none',
            }}
          >
            <div className="space-y-8 relative z-10">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                <div>
                  <label className="block text-[#1C1C1C] mb-3 text-xl font-bold">Full Name</label>
                  <input
                    type="text"
                    value={formData.fullName}
                    onChange={(e) => setFormData({ ...formData, fullName: e.target.value })}
                    onMouseEnter={() => setHoveredInput('fullName')}
                    onMouseLeave={() => setHoveredInput(null)}
                    placeholder="Enter your full name"
                    className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl shadow-inner"
                    style={{
                      borderColor: hoveredInput === 'fullName' ? '#D32F2F' : '#D1D5DB',
                      boxShadow: hoveredInput === 'fullName' ? '0 0 15px rgba(211, 47, 47, 0.2)' : 'none'
                    }}
                  />
                </div>

                <div>
                  <label className="block text-[#1C1C1C] mb-3 text-xl font-bold">Email Address</label>
                  <input
                    type="email"
                    value={formData.email}
                    onChange={(e) => setFormData({ ...formData, email: e.target.value })}
                    onMouseEnter={() => setHoveredInput('email')}
                    onMouseLeave={() => setHoveredInput(null)}
                    placeholder="name@example.com"
                    className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl shadow-inner"
                    style={{
                      borderColor: hoveredInput === 'email' ? '#1976D2' : '#D1D5DB',
                      boxShadow: hoveredInput === 'email' ? '0 0 15px rgba(25, 118, 210, 0.2)' : 'none'
                    }}
                  />
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                <div>
                  <label className="block text-[#1C1C1C] mb-3 text-xl font-bold">Phone Number</label>
                  <input
                    type="tel"
                    value={formData.phoneNumber}
                    onChange={(e) => setFormData({ ...formData, phoneNumber: e.target.value })}
                    onMouseEnter={() => setHoveredInput('phoneNumber')}
                    onMouseLeave={() => setHoveredInput(null)}
                    placeholder="+1 (555) 000-0000"
                    className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl shadow-inner"
                    style={{
                      borderColor: hoveredInput === 'phoneNumber' ? '#2E7D32' : '#D1D5DB',
                      boxShadow: hoveredInput === 'phoneNumber' ? '0 0 10px rgba(46, 125, 50, 0.2)' : 'none'
                    }}
                  />
                </div>

                <div>
                  <label className="block text-[#1C1C1C] mb-3 text-xl font-bold">Organization Name</label>
                  <input
                    type="text"
                    value={formData.organizationName}
                    onChange={(e) => setFormData({ ...formData, organizationName: e.target.value })}
                    onMouseEnter={() => setHoveredInput('organizationName')}
                    onMouseLeave={() => setHoveredInput(null)}
                    placeholder="Company or Institution"
                    className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl shadow-inner"
                    style={{
                      borderColor: hoveredInput === 'organizationName' ? '#7B1FA2' : '#D1D5DB',
                      boxShadow: hoveredInput === 'organizationName' ? '0 0 10px rgba(123, 31, 162, 0.2)' : 'none'
                    }}
                  />
                </div>
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-3 text-xl font-bold">Purpose of Data Access</label>
                <input
                  type="text"
                  value={formData.purpose}
                  onChange={(e) => setFormData({ ...formData, purpose: e.target.value })}
                  onMouseEnter={() => setHoveredInput('purpose')}
                  onMouseLeave={() => setHoveredInput(null)}
                  placeholder="Describe how you will use the data"
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl shadow-inner"
                  style={{
                    borderColor: hoveredInput === 'purpose' ? '#F57C00' : '#D1D5DB',
                    boxShadow: hoveredInput === 'purpose' ? '0 0 10px rgba(245, 124, 0, 0.2)' : 'none'
                  }}
                />
              </div>

              <button
                onClick={() => navigate("/user/organization-type")}
                onMouseEnter={() => setHoveredInput('next')}
                onMouseLeave={() => setHoveredInput(null)}
                className="lift-on-hover w-full bg-[#2E7D32] text-white py-5 rounded-xl border-2 cursor-pointer transition-all duration-300 text-2xl font-black shadow-lg mt-6"
                style={{
                  borderColor: hoveredInput === 'next' ? '#F4A300' : '#2E7D32',
                  boxShadow: hoveredInput === 'next' ? '0 15px 40px rgba(244, 163, 0, 0.4)' : 'none'
                }}
              >
                Continue to Next Step
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}