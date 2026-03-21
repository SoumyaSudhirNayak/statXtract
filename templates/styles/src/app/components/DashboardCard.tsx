import { ArrowRight, LucideIcon } from "lucide-react";
import { useNavigate } from "react-router";
import { useState } from "react";

interface DashboardCardProps {
  title: string;
  description: string;
  icon: LucideIcon;
  path: string;
  borderColor?: string;
}

export function DashboardCard({ title, description, icon: Icon, path, borderColor }: DashboardCardProps) {
  const navigate = useNavigate();
  const [isHovered, setIsHovered] = useState(false);

  return (
    <div
      onClick={() => navigate(path)}
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
      className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-2xl p-10 cursor-pointer transition-all duration-300 relative overflow-hidden h-full flex flex-col justify-center min-h-[300px] shadow-xl"
      style={{ 
        borderColor: isHovered ? borderColor : '#D1D5DB',
        boxShadow: isHovered
          ? `0 25px 60px ${borderColor}33, 0 0 25px ${borderColor}22`
          : '0 4px 15px rgba(0, 0, 0, 0.05)',
      }}
    >
      <div className="flex items-start justify-between relative z-10 h-full">
        <div className="flex-1 flex flex-col h-full">
          <div className="flex items-center gap-6 mb-8">
            <div 
              className="w-20 h-20 flex items-center justify-center rounded-3xl transition-all duration-500 shadow-lg"
              style={{ 
                backgroundColor: isHovered ? borderColor : '#F5F7FA',
                transform: isHovered ? 'scale(1.15) rotate(8deg)' : 'none'
              }}
            >
              <Icon 
                className="w-10 h-10 transition-colors duration-300" 
                style={{ color: isHovered ? 'white' : borderColor }}
              />
            </div>
          </div>
          <h3 
            className="text-3xl mb-4 transition-all duration-300 tracking-tight" 
            style={{ 
              fontFamily: 'var(--font-head)', 
              fontWeight: 800,
              color: isHovered ? borderColor : '#1C1C1C'
            }}
          >
            {title}
          </h3>
          <p className="text-[#6B7280] m-0 text-xl font-medium leading-relaxed flex-1">
            {description}
          </p>
        </div>
        <ArrowRight 
          className="w-8 h-8 mt-2 transition-all duration-500" 
          style={{ 
            color: isHovered ? borderColor : '#D1D5DB',
            transform: isHovered ? 'translateX(8px) scale(1.2)' : 'none'
          }}
        />
      </div>
    </div>
  );
}