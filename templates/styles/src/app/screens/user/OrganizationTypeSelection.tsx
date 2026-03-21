import { useNavigate } from "react-router";
import { GraduationCap, FlaskConical, Building, BarChart3 } from "lucide-react";
import { BackButton } from "../../components/BackButton";
import { AnimatedBackground } from "../../components/AnimatedBackground";
import { useState, useRef } from "react";

export function OrganizationTypeSelection() {
  const navigate = useNavigate();
  const [hoveredCard, setHoveredCard] = useState<string | null>(null);
  const cardRefs = useRef<Record<string, HTMLDivElement | null>>({});

  const organizationTypes = [
    {
      type: "student",
      title: "Student",
      icon: GraduationCap,
      description: "Academic students pursuing research or coursework",
      path: "/user/verify/student",
      color: "#D32F2F", // Red
    },
    {
      type: "researcher",
      title: "Researcher",
      icon: FlaskConical,
      description: "Professional researchers and academicians",
      path: "/user/verify/researcher",
      color: "#1976D2", // Blue
    },
    {
      type: "private",
      title: "Private Organization",
      icon: Building,
      description: "Private companies and corporations",
      path: "/user/verify/private",
      color: "#7B1FA2", // Purple
    },
    {
      type: "analyst",
      title: "Analyst",
      icon: BarChart3,
      description: "Independent data analysts and consultants",
      path: "/user/verify/analyst",
      color: "#F57C00", // Orange
    },
  ];

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

        <div className="max-w-[1400px] mx-auto px-10 py-20">
          <div className="mb-10">
            <BackButton to="/user/create-account" />
          </div>
          
          <h2 className="text-center text-6xl text-[#1C1C1C] mb-16 font-black" style={{ fontFamily: 'var(--font-head)' }}>
            Tell Us Who You Are
          </h2>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-12 max-w-[1200px] mx-auto">
            {organizationTypes.map((org) => (
              <div
                key={org.type}
                ref={el => cardRefs.current[org.type] = el}
                onClick={() => navigate(org.path)}
                onMouseEnter={() => setHoveredCard(org.type)}
                onMouseLeave={() => setHoveredCard(null)}
                className="lift-on-hover bg-white/40 backdrop-blur-md border-2 rounded-2xl p-12 cursor-pointer transition-all duration-300 relative group overflow-hidden shadow-xl min-h-[320px] flex flex-col justify-center"
                style={{
                  borderColor: hoveredCard === org.type ? org.color : '#D1D5DB',
                  boxShadow: hoveredCard === org.type
                    ? `0 25px 60px ${org.color}22, 0 0 30px ${org.color}33`
                    : 'none',
                }}
              >
                <div className="flex flex-col items-center text-center gap-6 relative z-10">
                  <div 
                    className="w-28 h-28 flex items-center justify-center rounded-3xl transition-all duration-500 shadow-lg"
                    style={{
                      backgroundColor: hoveredCard === org.type ? org.color : '#F3F4F6',
                      transform: hoveredCard === org.type ? 'scale(1.1) rotate(5deg)' : 'none'
                    }}
                  >
                    <org.icon 
                      className="w-14 h-14 transition-colors duration-300" 
                      style={{ color: hoveredCard === org.type ? 'white' : org.color }}
                    />
                  </div>
                  <div>
                    <h3 
                      className="text-4xl mb-3 transition-colors duration-300 font-black" 
                      style={{ 
                        fontFamily: 'var(--font-head)', 
                        color: hoveredCard === org.type ? org.color : '#1C1C1C'
                      }}
                    >
                      {org.title}
                    </h3>
                    <p className="text-[#6B7280] m-0 text-xl leading-relaxed max-w-[300px]">
                      {org.description}
                    </p>
                  </div>
                </div>
                {/* Minimal background accent */}
                <div 
                  className="absolute bottom-0 right-0 w-48 h-48 -mr-24 -mb-24 rounded-full opacity-10 transition-all duration-500 group-hover:scale-150"
                  style={{ backgroundColor: org.color }}
                />
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}